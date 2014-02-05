(ns lead.cloudwatch.connector
  (:require [lead.connector :as connector]
            [lead.matcher :as matcher]
            [lead.series :as series])
  (:import (com.amazonaws.auth BasicAWSCredentials)
           (com.amazonaws.services.cloudwatch AmazonCloudWatchClient)
           (com.amazonaws.services.cloudwatch.model GetMetricStatisticsRequest Dimension Datapoint)
           (java.util Date)))

(def instance-metrics
  ["NetworkIn"
   "CPUUtilization"
   "DiskWriteBytes"
   "StatusCheckFailed"
   "DiskReadBytes"
   "StatusCheckFailed_System"
   "StatusCheckFailed_Instance"
   "DiskReadOps"
   "DiskWriteOps"
   "NetworkOut"])

(def volume-metrics
  ["VolumeIdleTime"
   "VolumeTotalReadTime"
   "VolumeQueueLength"
   "VolumeWriteBytes"
   "VolumeWriteOps"
   "VolumeTotalWriteTime"
   "VolumeReadBytes"
   "VolumeReadOps"])

(def elb-metrics
  ["HealthyHostCount"
   "UnHealthyHostCount"
   "RequestCount"
   "Latency"
   "HTTPCode_ELB_4XX"
   "HTTPCode_ELB_5XX"
   "HTTPCode_Backend_2XX"
   "HTTPCode_Backend_3XX"
   "HTTPCode_Backend_4XX"
   "HTTPCode_Backend_5XX"
   "BackendConnectionErrors"
   "SurgeQueueLength"
   "SpilloverCount"])

(def statistics
  {"Sum"         (fn [^Datapoint dp] (.getSum dp))
   "Maximum"     (fn [^Datapoint dp] (.getMaximum dp))
   "Minimum"     (fn [^Datapoint dp] (.getMinimum dp))
   "SampleCount" (fn [^Datapoint dp] (.getSampleCount dp))
   "Average"     (fn [^Datapoint dp] (.getAverage dp))})

(defn- dimension-tree [key namespace dimension-name metrics]
  {:key key
   :children
        {:*
          {:namespace      namespace
           :dimension-name dimension-name
           :children
                           (into {}
                                 (map
                                   (fn [metric-name]
                                     [metric-name {:metric-name metric-name
                                                   :children    (into {}
                                                                      (map
                                                                        (fn [[statistic f]]
                                                                          [statistic {:statistic      statistic
                                                                                      :value-function f}])
                                                                        statistics))}])
                                   metrics))}}})

(def tree
  {:children
    {"instance" (dimension-tree "instance" "AWS/EC2" "InstanceId" instance-metrics)
     "volume"   (dimension-tree "volume" "AWS/EBS" "VolumeId" volume-metrics)
     "elb"      (dimension-tree "elb" "AWS/ELB" "LoadBalancerName" elb-metrics)}})

(def finder (matcher/->MapTreeFinder tree))

(defn basic-credentials [access-key secret-key]
  (BasicAWSCredentials. access-key secret-key))

(defn client [credentials]
  (AmazonCloudWatchClient. credentials))

(defn get-metric-statistics [client opts step spec]
  (let [request (doto (GetMetricStatisticsRequest.)
                  (.setMetricName (:metric-name spec))
                  (.setNamespace (:namespace spec))
                  (.setDimensions [(doto (Dimension.) (.setName (:dimension-name spec)) (.setValue (:dimension-value spec)))])
                  (.setStatistics [(:statistic spec)])
                  (.setStartTime (Date. (* 1000 (:start opts))))
                  (.setEndTime (Date. (* 1000 (:end opts))))
                  (.setPeriod (int step)))]
    (.getMetricStatistics client request)))

(defn- bucket [opts step datapoints value-fn]
  (let [start (:start opts)
        duration (- (:end opts) start)
        buckets (int (Math/ceil (/ duration (float step))))
        values (make-array Number buckets)]
    (doseq [dp datapoints]
      (let [ts (quot (.getTime (.getTimestamp dp)) 1000)
            b (quot (- ts start) step)]
        (aset values b (value-fn dp))))
    (seq values)))

(defrecord CloudWatchConnector [client]
  connector/Connector
  (query [this pattern] nil)
  (load [this target opts]
    (let [step (* 60 5)
          path (series/name->path target)
          dimension-value (get path 1)
          results (matcher/tree-find finder target)]
      (filter identity
              (pmap (fn [result]
                      (if (:is-leaf result)
                        (let [node-path (:path result)
                              spec (apply merge {:dimension-value dimension-value} (matcher/tree-traverse finder node-path))
                              stats (get-metric-statistics client opts step spec)]
                          {:name   (str (:key spec) \. dimension-value \. (:metric-name spec) \. (:statistic spec))
                           :step   step
                           :start  (:start opts)
                           :end    (:end opts)
                           :values (bucket opts step (.getDatapoints stats) (:value-function spec))})))
                    results)))))

(defn cloudwatch-connector [access-key secret-key]
  (->CloudWatchConnector (client (basic-credentials access-key secret-key))))
