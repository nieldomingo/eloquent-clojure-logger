(ns eloquent-clojure-logger.core
    (:require [aleph.tcp :as tcp]
              [clj-time.coerce]
              [clj-time.core]
              [manifold.stream :as s]
              [msgpack.core :as msg])
    (:import java.io.ByteArrayOutputStream))
            

(defn- event-time []
 (quot
   (clj-time.coerce/to-long
     (clj-time.core/now))
   1000))

(defn- event-stream [events]
  (let [out (ByteArrayOutputStream.)]
    (doseq [event events]
      (.write out (msg/pack [(event-time) event])))
    (.toByteArray out)))

(defn to-pack-forward
  ([tag messages] (to-pack-forward tag messages nil))
  ([tag messages options]
    (msg/pack [tag (event-stream messages) {"size" (count messages)}])
  ))

(defn eloquent-client [& {:keys [host port]
                         :or {host "127.0.0.1"
                              port 24224}}]
  @(tcp/client {:host host :port port}))

(defn eloquent-log [client tag event]
  @(s/put! client (to-pack-forward tag [event])))
