;; -*- coding: utf-8 -*-

(ns trident-in-clojure.core
  "CLI / server bootstrap."
  (:import [backtype.storm Config StormSubmitter LocalCluster LocalDRPC]
           [backtype.storm.spout SchemeAsMultiScheme]
           [backtype.storm.tuple Fields Tuple]
           [storm.trident TridentTopology]
           [storm.trident.tuple TridentTupleView]
           [storm.trident.testing MemoryMapState$Factory]
           [storm.trident.operation.builtin Count Sum MapGet FilterNull]
           [storm.kafka.trident OpaqueTridentKafkaSpout
            TridentKafkaConfig]
           [storm.kafka SpoutConfig KafkaConfig$ZkHosts StringScheme])
  (:require [trident-in-clojure.word-splitter]
            [clojure.tools.logging :as log])
  (:use [backtype.storm clojure config])
  (:gen-class))

(def config
  {:topic "words"
   :kafka {:zookeeper "127.0.0.1:2181"
           :broker-path "/brokers"}})

(defn create-kafka-spout
  [{:keys [kafka topic]}]
  (let [{:keys [zookeeper broker-path]} kafka
        zk-hosts (KafkaConfig$ZkHosts. zookeeper broker-path)
        spout-config (TridentKafkaConfig. zk-hosts topic)]
    (set! (.scheme spout-config) (SchemeAsMultiScheme. (StringScheme.)))
    (OpaqueTridentKafkaSpout. spout-config)))

(defn mk-drpc-stream
  [drpc topology wordcounts]
  (-> (.newDRPCStream topology "words" drpc)
      (.each (Fields. ["args"]) (com.whoahbot.trident.WordSplitter.) (Fields. ["word"]))
      (.groupBy (Fields. ["word"]))
      (.stateQuery wordcounts (Fields. ["word"])
                   (storm.trident.operation.builtin.MapGet.) (Fields. ["count"]))
      (.each (Fields. ["count"]) (FilterNull.))
      (.aggregate (Fields. ["count"]) (Sum.) (Fields. ["sum"]))))

(defn mk-wordcount-topology
  [topology config]
  (-> topology
      (.newStream "word-spout"
                  (create-kafka-spout config))
      (.each (Fields. ["str"])
             (com.whoahbot.trident.WordSplitter.)
             (Fields. ["word"]))
      (.groupBy (Fields. ["word"]))
      (.persistentAggregate (MemoryMapState$Factory.) (Count.) (Fields. ["count"]))))

(defn run-local-topology
  [config drpc]
  (let [cluster (LocalCluster.)
        cluster-config (Config.)
        topology (TridentTopology.)
        wordcounts (mk-wordcount-topology topology config)]
    (mk-drpc-stream drpc topology wordcounts)
    ;;(.setDebug cluster-config true)
    (.submitTopology cluster "wordcount-local-topology"
                     cluster-config
                     (.build topology))))

(defn -main
  [& args]
  (let [drpc (LocalDRPC.)]
    (run-local-topology config drpc)
    (while true
      (log/infof "Word count: %s" (.execute drpc "words" "baby"))
      (Thread/sleep 1000))))
