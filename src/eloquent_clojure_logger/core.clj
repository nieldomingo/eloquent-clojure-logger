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

(defn- gen-chunk-id []
  (.encodeToString base64-encoder (uuid/to-byte-array (uuid/v4))))

(defn to-pack-forward
  ([tag messages] (to-pack-forward tag messages {}))
  ([tag messages options]
    (msg/pack [tag
                (event-stream messages)
                (merge
                 options
                 {"size" (count messages)})])))

(defn eloquent-client [& {:keys [host port tag flush-interval max-chunk-cnt-size
                                 buffer-cnt-size]
                          :or {host "127.0.0.1"
                               port 24224
                               tag "my.tag"
                               flush-interval 10000
                               max-chunk-cnt-size 2
                               buffer-cnt-size 1000}}]
  (let [buffer-stream (s/stream buffer-cnt-size)
        output-stream (s/stream 1000)
        client @(tcp/client {:host host :port port})
        take-timeout 100
        flush-interval-ns (* 1000000 flush-interval)]
    (d/loop [message-chunk []
             ref-time-ns (uuid/monotonic-time)]
      (d/chain
        (s/try-take! buffer-stream ::drained take-timeout ::timeout)
        (fn [message]
          (if (or
                (identical? message ::drained)
                (identical? message ::timeout))
            message-chunk
            (conj message-chunk (encode-event message))))
        (fn [message-chunk]
          (cond
            (or
              (>= (- (uuid/monotonic-time) ref-time-ns) flush-interval-ns)
              (= (count message-chunk) max-chunk-cnt-size))
            [message-chunk ::flush]
            :else [message-chunk ::no-flush]))
        (fn [[message-chunk flush-flag]]
          (if (identical? ::flush flush-flag)
            (do
              @(s/put! output-stream message-chunk)
              (d/recur [] (uuid/monotonic-time)))
            (d/recur message-chunk ref-time-ns)))))
    (d/loop []
      (d/chain
        (s/take! output-stream)
        (fn [message-chunk]
          (s/put!
            client
            (to-pack-forward tag message-chunk)
            ))
        (fn [& args]
          (d/recur)
          )))
    buffer-stream))

(defn eloquent-log [client event]
  @(s/put! client event))
