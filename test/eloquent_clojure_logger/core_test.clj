(ns eloquent-clojure-logger.core-test
  (:require [aleph.tcp :as tcp]
            [clj-time.core]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [eloquent-clojure-logger.core :refer :all]
            [manifold.stream :as s]
            [msgpack.core :as msg]))

(defn- extract-events
  ([s] (extract-events s []))
  ([s events]
    (if (> (.available s) 0)
      (recur s (conj events (msg/unpack s)))
      events)))

(deftest test-encode-event
  (testing "Testing encode-event"
    (let [encoded-event (clj-time.core/do-at
                          (clj-time.core/date-time 2016 1 4)
                          (encode-event {}))
          event-array (msg/unpack encoded-event)]
        (is (= (first event-array) 1451865600))
        (is (= (second event-array) {}))
        )
    (let [encoded-event (clj-time.core/do-at
                          (clj-time.core/date-time 2016 1 5)
                          (encode-event {"message" "hello there"}))
          event-array (msg/unpack encoded-event)]
        (is (= (first event-array) 1451952000))
        (is (= (second event-array) {"message" "hello there"})))))

(def message (byte-array (map (comp byte int) "ascii")))

(deftest test-pack-foward
  (testing "Testing outermost envelope."
    (let [encoded (to-pack-forward "my.tag" [message])
          outermost (msg/unpack encoded)
          options (last outermost)]
      (is (= (count outermost) 3))
      (is (= (first outermost) "my.tag"))
      (is (map? options))
      (is (= (options "size") 1))
      (is (not (contains? options "chunk"))))
    (let [encoded (to-pack-forward
                   "my.other.tag"
                   [message message]
                   {"chunk" "chunk_id"})
          outermost (msg/unpack encoded)
          options (last outermost)]
      (is (= (first outermost) "my.other.tag"))
      (is (= (options "size") 2))
      (is (= (options "chunk") "chunk_id"))))
  (testing "Testing event-stream"
    (let [encoded (to-pack-forward
                    "my.tag"
                    [message])
          outermost (msg/unpack encoded)
          encoded-event-stream (second outermost)]
      (is (= (String. (bytes encoded-event-stream)) "ascii")))
    (let [encoded (to-pack-forward
                    "my.tag"
                    [message message])
          outermost (msg/unpack encoded)
          encoded-event-stream (second outermost)]
      (is (= (String. (bytes encoded-event-stream)) "asciiascii")))
      ))

(deftest test-eloquent-logging
  (testing "testing max-chunk-cnt-size"
    (let [tcp-client-stream (s/stream)]
      (with-redefs [tcp/client (fn [&args] (future tcp-client-stream))
                    encode-event (fn [message] message)
                    to-pack-forward (fn [tag message-chunk] message-chunk)]
        (let [client (eloquent-client
                       :flush-interval 1000000
                       :max-chunk-cnt-size 5)]
          (dotimes [n 12] (eloquent-log client ""))
          (let [chunk1 @(s/try-take! tcp-client-stream ::drained 1 ::timeout)
                chunk2 @(s/try-take! tcp-client-stream ::drained 1 ::timeout)
                chunk3 @(s/try-take! tcp-client-stream ::drained 1 ::timeout)]
            (is (= (count chunk1) 5))
            (is (= (count chunk2) 5))
            (is (identical? chunk3 ::timeout)))))))
  (testing "testing flush-interval"
    (let [tcp-client-stream (s/stream)]
      (with-redefs [tcp/client (fn [&args] (future tcp-client-stream))
                    encode-event (fn [message] message)
                    to-pack-forward (fn [tag message-chunk] message-chunk)]
        (let [client (eloquent-client
                       :flush-interval 1
                       :max-chunk-cnt-size 5)]
          (dotimes [n 3] (eloquent-log client ""))
          (let [chunk1 @(s/try-take! tcp-client-stream ::drained 100 ::timeout)]
            (is (and (coll? chunk1) (= (count chunk1) 3)))
            ))))))
