(ns eloquent-clojure-logger.core
    (:refer-clojure :exclude [uuid?])
    (:require [aleph.tcp :as tcp]
              [clj-time.coerce]
              [clj-time.core]
              [clj-uuid :as uuid]
              [manifold.deferred :as d]
              [manifold.stream :as s]
              [msgpack.core :as msg])
    (:import java.io.ByteArrayOutputStream))
            

(def base64-encoder (java.util.Base64/getEncoder))

(defn- event-time []
 (quot
   (clj-time.coerce/to-long
     (clj-time.core/now))
   1000))

(defn encode-event [event]
  (msg/pack [(event-time) event]))

(defn- event-stream [events]
  (let [out (ByteArrayOutputStream.)]
    (doseq [event events]
      (.write out event))
    (.toByteArray out)))

(defn gen-chunk-id []
  (.encodeToString base64-encoder (uuid/to-byte-array (uuid/v4))))

(defn package-message-chunk
  ([tag messages] (package-message-chunk tag messages {}))
  ([tag messages options]
    (msg/pack [tag
                (event-stream messages)
                (merge
                 options
                 {"size" (count messages)})])))

(defn- message-aggregator [input-stream
                           message-chunk-stream
                           flush-interval
                           max-chunk-cnt-size]
    (let [take-timeout 100
          flush-interval-ns (* 10000 flush-interval)
          is-not-message? (fn [message]
                            (or
                              (identical? message ::drained)
                              (identical? message ::timeout)))
          should-flush? (fn [message-chunk ref-time-ns]
                          (or
                            (>=
                              (-
                                (uuid/monotonic-time)
                                ref-time-ns)
                                flush-interval-ns)
                            (=
                              (count message-chunk)
                              max-chunk-cnt-size)))]
      (d/loop [message-chunk []
               ref-time-ns (uuid/monotonic-time)]
        (d/chain
          (s/try-take! input-stream ::drained take-timeout ::timeout)
          (fn [message]
            (if (is-not-message? message)
              message-chunk
              (conj message-chunk (encode-event message))))
          (fn [message-chunk]
            (if (should-flush? message-chunk ref-time-ns)
              [message-chunk ::flush]
              [message-chunk ::no-flush]))
          (fn [[message-chunk flush-flag]]
            (if (identical? ::flush flush-flag)
              (do
                @(s/put! message-chunk-stream message-chunk)
                (d/recur [] (uuid/monotonic-time)))
              (d/recur message-chunk ref-time-ns)))))))

(defn- send-with-acks [host port tag ack-resend-time]
  (let [receiver @(tcp/client {:host host :port port})] 
    (fn [message-chunk]
      (let [chunk-id (gen-chunk-id)
            get-response (fn [& args]
                           (s/try-take!
                             receiver
                             ::drained
                             ack-resend-time
                             ::timeout))
            send-chunk (fn [message-chunk]
                         (s/put!
                           receiver
                           (package-message-chunk
                             tag
                             message-chunk
                             {"chunk" chunk-id})))
            valid-response-id? (fn [response chunk-id]
                                (=
                                  chunk-id
                                  ((msg/unpack response) "ack")))]
        (d/loop []
          (d/chain
            (send-chunk message-chunk)
            get-response
            (fn [response]
              (if (or
                    (identical? response ::timeout)
                    (not (valid-response-id? response chunk-id)))
                 (d/recur)))))))))

(defn- send-fire-forget [host port tag]
  (let [receiver @(tcp/client {:host host :port port})]
    (fn [message-chunk]
    (s/put!
      receiver
      (package-message-chunk tag message-chunk)))))

(defn- at-least-once-sender [message-chunk-stream host port tag ack-resend-time]
  (d/loop []
    (d/chain
      (s/take! message-chunk-stream)
      (send-with-acks host port tag ack-resend-time)
      (fn [& args]
        (d/recur)))))

(defn- at-most-once-sender [message-chunk-stream host port tag]
  (d/loop []
    (d/chain
      (s/take! message-chunk-stream)
      (send-fire-forget host port tag)
      (fn [& args]
        (d/recur)))))

(defn eloquent-client [& {:keys [host port tag flush-interval max-chunk-cnt-size
                                 buffer-cnt-size use-ack ack-resend-time]
                          :or {host "127.0.0.1"
                               port 24224
                               tag "my.tag"
                               flush-interval 10000
                               max-chunk-cnt-size 100
                               buffer-cnt-size 1000
                               use-ack false
                               ack-resend-time 60000}}]
  (let [buffer-stream (s/stream buffer-cnt-size)
        output-stream (s/stream 1000)
        ;client @(tcp/client {:host host :port port})
        ]
    (message-aggregator
      buffer-stream
      output-stream
      flush-interval
      max-chunk-cnt-size)
    (if use-ack
      (at-least-once-sender
        output-stream
        host
        port
        tag
        ack-resend-time)
      (at-most-once-sender
        output-stream
        host
        port
        tag))
    buffer-stream))

(defn eloquent-log [client event]
  @(s/put! client event))
