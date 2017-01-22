(ns eloquent-clojure-logger.core
    (:refer-clojure :exclude [uuid?])
    (:require [aleph.tcp :as tcp]
              [clj-time.coerce]
              [clj-time.core]
              [clj-uuid :as uuid]
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

(defn eloquent-client [& {:keys [host port]
                         :or {host "127.0.0.1"
                              port 24224}}]
  @(tcp/client {:host host :port port}))

(defn eloquent-log [client tag event]
  @(s/put! client (to-pack-forward tag [(encode-event event)])))

