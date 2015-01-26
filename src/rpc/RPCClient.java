package rpc;

import messaging.HybridEngine.HybridStoreService;
import messaging.HybridEngine.HybridStoreService.BlockingInterface;

import com.google.protobuf.BlockingRpcChannel;
import com.googlecode.protobuf.socketrpc.RpcChannels;
import com.googlecode.protobuf.socketrpc.RpcConnectionFactory;
import com.googlecode.protobuf.socketrpc.SocketRpcConnectionFactories;

public class RPCClient {
	RpcConnectionFactory connectionFactory;
	BlockingRpcChannel channel;
	
	public RPCClient(String host, int port) {
		connectionFactory = SocketRpcConnectionFactories
			    .createRpcConnectionFactory(host, port);
	}
	
	public BlockingInterface getClientBlockingInterface() {
		channel = RpcChannels.newBlockingRpcChannel(connectionFactory);
		BlockingInterface service = HybridStoreService.
				newBlockingStub(channel);
		return service;
	}

}
