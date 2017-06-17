(ns hdfs-client.core
  (:require [taoensso.timbre :refer [info debug warn set-level!]])
  (:import [org.apache.hadoop.hdfs DFSClient]
           [org.apache.hadoop.conf Configuration]
           [java.net URI]
           [org.apache.hadoop.hdfs.protocol HdfsFileStatus]))

(set-level! :trace) 
(comment
  (import '[java.net InetSocketAddress])
  (import '[java.io BufferedInputStream BufferedOutputStream ByteArrayOutputStream
            DataOutputStream DataInputStream])
  (import '[com.google.protobuf ByteString Message])
  (import '[org.apache.hadoop.ipc.protobuf
            RpcHeaderProtos
            RpcHeaderProtos$RpcKindProto
            RpcHeaderProtos$RpcRequestHeaderProto
            RpcHeaderProtos$RpcRequestHeaderProto$OperationProto
            RpcHeaderProtos$RpcSaslProto
            RpcHeaderProtos$RpcSaslProto$SaslState
            RpcHeaderProtos$RpcResponseHeaderProto
            IpcConnectionContextProtos$IpcConnectionContextProto
            IpcConnectionContextProtos$UserInformationProto
            ProtobufRpcEngineProtos
            ProtobufRpcEngineProtos$RequestHeaderProto
            ])
  (import '[org.apache.hadoop.hdfs.protocol.proto
            ClientNamenodeProtocolProtos
            ClientNamenodeProtocolProtos$GetListingRequestProto
            ClientNamenodeProtocolProtos$GetListingResponseProto])

    (def socket (.createSocket (javax.net.SocketFactory/getDefault)) )
    (doto socket (.setKeepAlive true ))
    (.connect socket (new InetSocketAddress "192.168.1.3" 9000)  )
    (doto socket (.setSoTimeout 0))

    (def header (-> (doto (RpcHeaderProtos$RpcRequestHeaderProto/newBuilder)
                        (.setRpcOp RpcHeaderProtos$RpcRequestHeaderProto$OperationProto/RPC_FINAL_PACKET)
                        (.setRpcKind RpcHeaderProtos$RpcKindProto/RPC_PROTOCOL_BUFFER)
                        (.setCallId -3) ; RpcConstants.CONNECTION_CONTEXT_CALL_ID
                        (.setClientId (ByteString/copyFrom (byte-array [])) )   ; #RpcConstants.DUMMY_CLIENT_ID
                        (.setRetryCount -1) ; RpcConstants.INVALID_RETRY_COUNT
                        ) .build))
    
    (def context (-> (doto (IpcConnectionContextProtos$IpcConnectionContextProto/newBuilder)
                       (.setProtocol "org.apache.hadoop.hdfs.protocol.ClientProtocol")
                       (.setUserInfo (-> (doto (IpcConnectionContextProtos$UserInformationProto/newBuilder)
                                           (.setEffectiveUser "spiderdt")
                                           ) .build)  )
                       ) .build) )
    
    (def call-header (-> (doto (RpcHeaderProtos$RpcRequestHeaderProto/newBuilder)
                    (.setRpcOp RpcHeaderProtos$RpcRequestHeaderProto$OperationProto/RPC_FINAL_PACKET)
                    (.setRpcKind RpcHeaderProtos$RpcKindProto/RPC_PROTOCOL_BUFFER)
                    (.setCallId 0) ; 
                    (.setClientId (ByteString/copyFrom (byte-array []))) ; RpcConstants.DUMMY_CLIENT_ID
                    (.setRetryCount 0) ; 
                    ) .build))
    (def call-method (-> (doto (ProtobufRpcEngineProtos$RequestHeaderProto/newBuilder)
                           (.setMethodName "getListing")
                           (.setDeclaringClassProtocolName "org.apache.hadoop.hdfs.protocol.ClientProtocol")
                           (.setClientProtocolVersion 1)
                           ) .build))
    (def call-args (-> (doto (ClientNamenodeProtocolProtos$GetListingRequestProto/newBuilder)
                         (.setSrc "/user")
                         (.setStartAfter (ByteString/copyFrom (.getBytes "")))
                         (.setNeedLocation true)
                         ) .build))
    
    (def out (->> (.getOutputStream socket) (new BufferedOutputStream) (new DataOutputStream)) )
    (doto out (.writeBytes "hrpc")        ; RpcConstants.HEADER
          (.writeByte 9)  ; RpcConstants.CURRENT_VERSION
          (.writeByte 0)  ; RPC.RPC_SERVICE_CLASS_DEFAULT
          (.writeByte 0)  ; Server.AuthProtocol[NONE(0), SASL(-33)]
          )
    (do (def bos (new ByteArrayOutputStream))
        (.writeDelimitedTo header bos)
        (.writeDelimitedTo context bos)
        (.writeInt out (.size bos))
        (.writeTo bos out))
    (do (def bos (new ByteArrayOutputStream))
        (.writeDelimitedTo call-header bos)
        (.writeDelimitedTo call-method bos)
        (.writeDelimitedTo call-args bos)
        (.writeInt out (.size bos))
        (.writeTo bos out))
    (.flush out)    
    
    (def in (->> (.getInputStream socket) (new BufferedInputStream) (new DataInputStream)) )
    (def length (.readInt in))
    (def resp-header (RpcHeaderProtos$RpcResponseHeaderProto/parseDelimitedFrom in) )
    (def resp (ClientNamenodeProtocolProtos$GetListingResponseProto/parseDelimitedFrom in))
    (prn resp) 
    )
