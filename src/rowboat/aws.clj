(ns rowboat.aws
  (:import (scala None$)
           (java.util HashMap)
           (akka.actor ActorSystem)
           (akka.stream.alpakka.s3 S3Ext)
           (akka.stream.alpakka.s3.scaladsl S3)
           (software.amazon.awssdk.regions Region)
           (akka.http.scaladsl.model ContentTypes)
           (akka.stream.alpakka.s3.headers CannedAcl)
           (akka.stream.alpakka.s3 MetaHeaders S3Attributes)
           (software.amazon.awssdk.regions.providers AwsRegionProvider)
           (software.amazon.awssdk.auth.credentials AwsBasicCredentials AwsCredentialsProvider
                                                    DefaultCredentialsProvider ProfileCredentialsProvider)))

(defn- aws-credentials-provider [{:keys [access-key-id secret-key profile]}]
  (if (some empty? [access-key-id secret-key])
    (if profile
      (ProfileCredentialsProvider/create profile)
      (DefaultCredentialsProvider/create))
    (reify AwsCredentialsProvider
      (resolveCredentials [_]
        (AwsBasicCredentials/create access-key-id secret-key)))))

(defn- aws-region-provider [endpoint]
  (reify AwsRegionProvider
    (getRegion [_]
      (Region/of endpoint))))

(defn s3-settings [system config]
  (let [creds  (aws-credentials-provider config)
        region (aws-region-provider (:region config))]
    (-> (S3Ext/get ^ActorSystem system)
        .settings
        (.withCredentialsProvider creds)
        (.withS3RegionProvider region))))

(defn s3-sink [s3-settings bucket filename {:keys [content-type chunking-parallelism]}]
  (-> (S3/multipartUpload
        bucket
        filename
        ;; the default arguments follow, except for parallelism
        (case content-type
          :csv   (ContentTypes/text$divcsv$u0028UTF$minus8$u0029)
          :plain (ContentTypes/text$divplain$u0028UTF$minus8$u0029)
          :html  (ContentTypes/text$divhtml$u0028UTF$minus8$u0029)
          :xml   (ContentTypes/text$divxml$u0028UTF$minus8$u0029)
          :json  (ContentTypes/application$divjson)
          (ContentTypes/application$divoctet$minusstream))
        (MetaHeaders/create (HashMap.))
        (CannedAcl/Private)
        (S3/MinChunkSize)
        chunking-parallelism
        None$/MODULE$)
      (.withAttributes (S3Attributes/settings s3-settings))))
