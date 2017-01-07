(ns eloquent-clojure-logger.core-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [eloquent-clojure-logger.core :refer :all]
            [msgpack.core :as msg])
  )

(def
  messages
  [{
    "message" "this is one"
  }
  {
    "message" "this is two"
  }])

(defn- extract-events
  ([s] (extract-events s []))
  ([s events]
    (if (> (.available s) 0)
      (recur s (conj events (msg/unpack s)))
      events)))

(deftest test-pack-foward
  (testing "Testing outermost envelope."
    (let [encoded (to-pack-forward "my.tag" messages)
          outermost (msg/unpack encoded)]
      (is (= (count outermost) 3))
      (is (= (first outermost) "my.tag"))
      (is (map? (last outermost)))
      (is (= ((last outermost) "size") 2)))
    (let [encoded (to-pack-forward "my.other.tag" (concat messages messages))
          outermost (msg/unpack encoded)]
      (is (= (first outermost) "my.other.tag"))
      (is (= ((last outermost) "size") 4))))
  (testing "Testing event-stream"
    (let [encoded (to-pack-forward "my.tag" messages)
          outermost (msg/unpack encoded)
          encoded-event-stream (second outermost)
          events (extract-events (io/input-stream encoded-event-stream))]
      (is (= (count events) 2))
      (is (= (count (first events)) 2))
      (is (integer? (first (first events))))
      )
    (let [encoded (to-pack-forward "my.tag" (concat messages messages))
          outermost (msg/unpack encoded)
          encoded-event-stream (second outermost)
          events (extract-events (io/input-stream encoded-event-stream))]
      (is (= (count events) 4)))
      )
  (testing "Testing event"
    (let [encoded (to-pack-forward "my.tag" messages)
          outermost (msg/unpack encoded)
          encoded-event-stream (second outermost)
          [first-event second-event] (extract-events (io/input-stream encoded-event-stream))]
      (is (integer? (first first-event)))
      (is (map? (second first-event)))
      (is (= ((second first-event) "message") "this is one"))
      (is (= ((second second-event) "message") "this is two")))))
