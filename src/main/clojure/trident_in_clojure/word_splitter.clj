;; -*- coding: utf-8 -*-

(ns trident-in-clojure.word-splitter
  "Function that takes a sentence as a kafka message,
   and returns new Values of words."
  (:import [storm.trident.operation TridentCollector Function]
           [backtype.storm.tuple Values])
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log])
  (:gen-class
   :name com.whoahbot.trident.WordSplitter
   :implements [storm.trident.operation.Function]))

(defn -prepare
  "Noop, required by interface."
  [this conf context])

(defn -execute
  "Deserialize a TridentTupleView to a string, and emit a new Value for each word
   in the string."
  [this ^storm.trident.tuple.TridentTupleView tuple ^TridentCollector collector]
  (let [sentence (.getString tuple 0)
        words (str/split sentence #"\s+")]
    (doseq [word words]
      (.emit collector (Values. (to-array word))))))