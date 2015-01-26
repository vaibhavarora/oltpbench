package com.oltpbenchmark.benchmarks.tpcc.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Random;

import messaging.HybridEngine.ApplicationClientRequest;
import messaging.HybridEngine.ApplicationClientResponse;
import messaging.HybridEngine.HybridStoreService.BlockingInterface;

import org.apache.log4j.Logger;

import rpc.RPCClient;
import utilities.Constants;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.socketrpc.SocketRpcController;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig;

public class NewOrder extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(NewOrder.class);

    public final SQLStmt stmtGetCustWhseSQL = new SQLStmt(
    		"SELECT C_DISCOUNT, C_LAST, C_CREDIT, W_TAX"
			+ "  FROM " + TPCCConstants.TABLENAME_CUSTOMER + ", " + TPCCConstants.TABLENAME_WAREHOUSE
			+ " WHERE W_ID = ? AND C_W_ID = ?"
			+ " AND C_D_ID = ? AND C_ID = ?");

    public final SQLStmt stmtGetDistSQL = new SQLStmt(
    		"SELECT D_NEXT_O_ID, D_TAX FROM " + TPCCConstants.TABLENAME_DISTRICT
					+ " WHERE D_W_ID = ? AND D_ID = ? FOR UPDATE"
    				);

	public final SQLStmt  stmtInsertNewOrderSQL = new SQLStmt("INSERT INTO "+ TPCCConstants.TABLENAME_NEWORDER + " (NO_O_ID, NO_D_ID, NO_W_ID) VALUES ( ?, ?, ?)");

	public final SQLStmt  stmtUpdateDistSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_DISTRICT + " SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_W_ID = ? AND D_ID = ?");

	public final SQLStmt  stmtInsertOOrderSQL = new SQLStmt("INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER
			+ " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)"
			+ " VALUES (?, ?, ?, ?, ?, ?, ?)");

	public final SQLStmt  stmtGetItemSQL = new SQLStmt("SELECT I_PRICE, I_NAME , I_DATA FROM " + TPCCConstants.TABLENAME_ITEM + " WHERE I_ID = ?");

	public final SQLStmt  stmtGetStockSQL = new SQLStmt("SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, "
			+ "       S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10"
			+ " FROM " + TPCCConstants.TABLENAME_STOCK + " WHERE S_I_ID = ? AND S_W_ID = ? FOR UPDATE");

	public final SQLStmt  stmtUpdateStockSQL = new SQLStmt("UPDATE " + TPCCConstants.TABLENAME_STOCK + " SET S_QUANTITY = ? , S_YTD = S_YTD + ?, S_ORDER_CNT = S_ORDER_CNT + 1, S_REMOTE_CNT = S_REMOTE_CNT + ? "
			+ " WHERE S_I_ID = ? AND S_W_ID = ?");

	public final SQLStmt  stmtInsertOrderLineSQL = new SQLStmt("INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE + " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID,"
			+ "  OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?,?,?,?,?,?,?,?,?)");


	// NewOrder Txn
	private PreparedStatement stmtGetCustWhse = null;
	private PreparedStatement stmtGetDist = null;
	private PreparedStatement stmtInsertNewOrder = null;
	private PreparedStatement stmtUpdateDist = null;
	private PreparedStatement stmtInsertOOrder = null;
	private PreparedStatement stmtGetItem = null;
	private PreparedStatement stmtGetStock = null;
	private PreparedStatement stmtUpdateStock = null;
	private PreparedStatement stmtInsertOrderLine = null;
	
	private String transactionalClientHost = "127.0.0.1";
	private int transactionalClientPort = 8081;
	
	//RPC addition
	RPCClient transactionClientRPCClient = new RPCClient(transactionalClientHost, 
            transactionalClientPort);
	Random rnGenerator = new Random();
 

    public ResultSet run(Connection conn, Random gen,
			int terminalWarehouseID, int numWarehouses,
			int terminalDistrictLowerID, int terminalDistrictUpperID,
			TPCCWorker w) throws SQLException {

		//initializing all prepared statements
		stmtGetCustWhse=this.getPreparedStatement(conn, stmtGetCustWhseSQL);
		stmtGetDist=this.getPreparedStatement(conn, stmtGetDistSQL);
		stmtInsertNewOrder=this.getPreparedStatement(conn, stmtInsertNewOrderSQL);
		stmtUpdateDist =this.getPreparedStatement(conn, stmtUpdateDistSQL);
		stmtInsertOOrder =this.getPreparedStatement(conn, stmtInsertOOrderSQL);
		stmtGetItem =this.getPreparedStatement(conn, stmtGetItemSQL);
		stmtGetStock =this.getPreparedStatement(conn, stmtGetStockSQL);
		stmtUpdateStock =this.getPreparedStatement(conn, stmtUpdateStockSQL);
		stmtInsertOrderLine =this.getPreparedStatement(conn, stmtInsertOrderLineSQL);


		int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
		int customerID = TPCCUtil.getCustomerID(gen);

		int numItems = (int) TPCCUtil.randomNumber(5, 15, gen);
		int[] itemIDs = new int[numItems];
		int[] supplierWarehouseIDs = new int[numItems];
		int[] orderQuantities = new int[numItems];
		int allLocal = 1;
		for (int i = 0; i < numItems; i++) {
			itemIDs[i] = TPCCUtil.getItemID(gen);
			if (TPCCUtil.randomNumber(1, 100, gen) > 1) {
				supplierWarehouseIDs[i] = terminalWarehouseID;
			} else {
				do {
					supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1,
							numWarehouses, gen);
				} while (supplierWarehouseIDs[i] == terminalWarehouseID
						&& numWarehouses > 1);
				allLocal = 0;
			}
			orderQuantities[i] = TPCCUtil.randomNumber(1, 10, gen);
		}

		// we need to cause 1% of the new orders to be rolled back.
		if (TPCCUtil.randomNumber(1, 100, gen) == 1)
			itemIDs[numItems - 1] = jTPCCConfig.INVALID_ITEM_ID;


		boolean transactionStatus = newOrderTransaction(terminalWarehouseID, districtID,
						customerID, numItems, allLocal, itemIDs,
						supplierWarehouseIDs, orderQuantities, conn, w);
		w.setTransactionStatus(transactionStatus);
		return null;

    }

	private boolean newOrderTransaction(int w_id, int d_id, int c_id,
			int o_ol_cnt, int o_all_local, int[] itemIDs,
			int[] supplierWarehouseIDs, int[] orderQuantities, Connection conn, TPCCWorker w)
			throws SQLException {
	    
	    
	    // assigning random transaction Ids for now
	    int transactionId = rnGenerator.nextInt(1000000);
        String beginRequest = "transactional "+ transactionId+ 
                " Begin";
	    
	    BlockingInterface service = transactionClientRPCClient.getClientBlockingInterface();
        RpcController controller = new SocketRpcController();
        String operation;
        String sqlString;
        
        ApplicationClientRequest applicationClientBeginRequest = ApplicationClientRequest.newBuilder().
                setDataOperation(beginRequest).build();
        
        try {
            service.processDataOperation(controller, applicationClientBeginRequest);
        } catch (ServiceException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        ApplicationClientRequest applicationClientOperationRequest;
        ApplicationClientResponse applicationClientOperationResponse = null; 
        
		float c_discount, w_tax, d_tax = 0, i_price;
		int d_next_o_id, o_id = -1, s_quantity;
		String c_last = null, c_credit = null, i_name, i_data, s_data;
		String s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05;
		String s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, ol_dist_info = null;
		float[] itemPrices = new float[o_ol_cnt];
		float[] orderLineAmounts = new float[o_ol_cnt];
		String[] itemNames = new String[o_ol_cnt];
		int[] stockQuantities = new int[o_ol_cnt];
		char[] brandGeneric = new char[o_ol_cnt];
		int ol_supply_w_id, ol_i_id, ol_quantity;
		int s_remote_cnt_increment;
		float ol_amount, total_amount = 0;
		try
		{
			stmtGetCustWhse.setInt(1, w_id);
			stmtGetCustWhse.setInt(2, w_id);
			stmtGetCustWhse.setInt(3, d_id);
			stmtGetCustWhse.setInt(4, c_id);
			
			//sqlString = ((JDBC4PreparedStatement)stmtGetCustWhse).getPreparedSql();
			sqlString = stmtGetCustWhse.toString();
            sqlString = sqlString.substring(sqlString.indexOf(":")+2);
			operation = "transactional "+ transactionId + " " + sqlString;
			applicationClientOperationRequest = ApplicationClientRequest.
			        newBuilder().setDataOperation(operation).build();
			try {
			    applicationClientOperationResponse = service.processDataOperation(controller, 
			            applicationClientOperationRequest);
			    if(!applicationClientOperationResponse.getOperationSuccessful()){
	                throw new RuntimeException("Aborting transaction");
	            }
            } catch (ServiceException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }		
			
			String resultString = applicationClientOperationResponse.getResultString();
			String[] resultRows = resultString.split(Constants.RESULT_ROW_DELIMITER);
			String[] resultTupleColumn = resultRows[0].split(Constants.
			        RESULT_COLUMN_DELIMITER);
			
			c_discount = Float.parseFloat(resultTupleColumn[0]);
			c_last = resultTupleColumn[1];
			c_credit = resultTupleColumn[2];
			w_tax = Float.parseFloat(resultTupleColumn[3]);


			stmtGetDist.setInt(1, w_id);
			stmtGetDist.setInt(2, d_id);
			
			//sqlString = ((JDBC4PreparedStatement)stmtGetDist).getPreparedSql();
			sqlString = stmtGetDist.toString();
            sqlString = sqlString.substring(sqlString.indexOf(":")+2);
			operation = "transactional "+ transactionId + " " + sqlString;
			
			applicationClientOperationRequest = ApplicationClientRequest.
                    newBuilder().setDataOperation(operation).build();
            try {
                applicationClientOperationResponse = service.
                        processDataOperation(controller, applicationClientOperationRequest);
                if(!applicationClientOperationResponse.getOperationSuccessful()){
                    throw new RuntimeException("Aborting transaction");
                }
            } catch (ServiceException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            resultString = applicationClientOperationResponse.getResultString();
            resultRows = resultString.split(Constants.RESULT_ROW_DELIMITER);
            resultTupleColumn = resultRows[0].split(Constants.
                    RESULT_COLUMN_DELIMITER);
			
			if (resultString.equals("")) {
				throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id
						+ " not found!");
			}
			d_next_o_id = Integer.parseInt(resultTupleColumn[0]);
			d_tax = Float.parseFloat(resultTupleColumn[1]);
			
			//woonhak, need to change order because of foreign key constraints
			//update next_order_id first, but it might doesn't matter
			stmtUpdateDist.setInt(1, w_id);
			stmtUpdateDist.setInt(2, d_id);
			//int result = stmtUpdateDist.executeUpdate();
			
			//sqlString = ((JDBC4PreparedStatement)stmtUpdateDist).getPreparedSql();
			sqlString = stmtUpdateDist.toString();
            sqlString = sqlString.substring(sqlString.indexOf(":")+2);
			operation = "transactional "+ transactionId + " " + sqlString;
            applicationClientOperationRequest = ApplicationClientRequest.
                    newBuilder().setDataOperation(operation).build();
            try {
                applicationClientOperationResponse = service.
                        processDataOperation(controller, applicationClientOperationRequest);
                if(!applicationClientOperationResponse.getOperationSuccessful()){
                    throw new RuntimeException("Aborting transaction");
                }
            } catch (ServiceException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            /*
			if (result == 0)
				throw new RuntimeException(
						"Error!! Cannot update next_order_id on district for D_ID="
								+ d_id + " D_W_ID=" + w_id);
            */
			o_id = d_next_o_id;

			// woonhak, need to change order, because of foreign key constraints
			//[[insert ooder first
			stmtInsertOOrder.setInt(1, o_id);
			stmtInsertOOrder.setInt(2, d_id);
			stmtInsertOOrder.setInt(3, w_id);
			stmtInsertOOrder.setInt(4, c_id);
			stmtInsertOOrder.setTimestamp(5,
					new Timestamp(System.currentTimeMillis()));
			stmtInsertOOrder.setInt(6, o_ol_cnt);
			stmtInsertOOrder.setInt(7, o_all_local);
			//stmtInsertOOrder.executeUpdate();
			
			//sqlString = ((JDBC4PreparedStatement)stmtInsertOOrder).getPreparedSql();
			sqlString = stmtInsertOOrder.toString();
            sqlString = sqlString.substring(sqlString.indexOf(":")+2);
			operation = "transactional "+ transactionId + " " + sqlString;
			applicationClientOperationRequest = ApplicationClientRequest.
                    newBuilder().setDataOperation(operation).build();
            try {
                applicationClientOperationResponse = service.processDataOperation
                        (controller, applicationClientOperationRequest);
                if(!applicationClientOperationResponse.getOperationSuccessful()){
                    throw new RuntimeException("Aborting transaction");
                }
            } catch (ServiceException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
			//insert ooder first]]
			/*TODO: add error checking */

			stmtInsertNewOrder.setInt(1, o_id);
			stmtInsertNewOrder.setInt(2, d_id);
			stmtInsertNewOrder.setInt(3, w_id);
			//stmtInsertNewOrder.executeUpdate();
			
			//sqlString = ((JDBC4PreparedStatement)stmtInsertNewOrder).getPreparedSql();
			sqlString = stmtInsertNewOrder.toString();
            sqlString = sqlString.substring(sqlString.indexOf(":")+2);
			operation = "transactional "+ transactionId + " " + sqlString;
			applicationClientOperationRequest = ApplicationClientRequest.
                    newBuilder().setDataOperation(operation).build();
            try {
                applicationClientOperationResponse = service.
                        processDataOperation(controller, applicationClientOperationRequest);
                if(!applicationClientOperationResponse.getOperationSuccessful()){
                    throw new RuntimeException("Aborting transaction");
                }
            } catch (ServiceException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
			/*TODO: add error checking */


			/* woonhak, [[change order				 
			stmtInsertOOrder.setInt(1, o_id);
			stmtInsertOOrder.setInt(2, d_id);
			stmtInsertOOrder.setInt(3, w_id);
			stmtInsertOOrder.setInt(4, c_id);
			stmtInsertOOrder.setTimestamp(5,
					new Timestamp(System.currentTimeMillis()));
			stmtInsertOOrder.setInt(6, o_ol_cnt);
			stmtInsertOOrder.setInt(7, o_all_local);
			stmtInsertOOrder.executeUpdate();
			change order]]*/

			for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
				ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
				ol_i_id = itemIDs[ol_number - 1];
				ol_quantity = orderQuantities[ol_number - 1];
				stmtGetItem.setInt(1, ol_i_id);
				
				//sqlString = ((JDBC4PreparedStatement)stmtGetItem).getPreparedSql();
				sqlString = stmtGetItem.toString();
	            sqlString = sqlString.substring(sqlString.indexOf(":")+2);
				operation = "transactional "+ transactionId + " " + sqlString;
				applicationClientOperationRequest = ApplicationClientRequest.
	                    newBuilder().setDataOperation(operation).build();
	            try {
	                applicationClientOperationResponse = service.
	                        processDataOperation(controller, applicationClientOperationRequest);
	                if(!applicationClientOperationResponse.getOperationSuccessful()){
	                    throw new RuntimeException("Aborting transaction");
	                }
	            } catch (ServiceException e) {
	                // TODO Auto-generated catch block
	                e.printStackTrace();
	            }
	            
	            resultString = applicationClientOperationResponse.getResultString();
	            resultRows = resultString.split(Constants.RESULT_ROW_DELIMITER);
	            resultTupleColumn = resultRows[0].split(Constants.
	                    RESULT_COLUMN_DELIMITER);
	            
				if (resultString.equals("")) {
					// This is (hopefully) an expected error: this is an
					// expected new order rollback
					assert ol_number == o_ol_cnt;
					assert ol_i_id == jTPCCConfig.INVALID_ITEM_ID;
					throw new UserAbortException(
							"EXPECTED new order rollback: I_ID=" + ol_i_id
									+ " not found!");
				}

				i_price = Float.parseFloat(resultTupleColumn[0]);
				i_name = resultTupleColumn[1];
				i_data = resultTupleColumn[2];

				itemPrices[ol_number - 1] = i_price;
				itemNames[ol_number - 1] = i_name;


				stmtGetStock.setInt(1, ol_i_id);
				stmtGetStock.setInt(2, ol_supply_w_id);
				
				//sqlString = ((JDBC4PreparedStatement)stmtGetStock).getPreparedSql();
				sqlString = stmtGetStock.toString();
	            sqlString = sqlString.substring(sqlString.indexOf(":")+2);
				operation = "transactional "+ transactionId + " " + sqlString;
				applicationClientOperationRequest = ApplicationClientRequest.
                        newBuilder().setDataOperation(operation).build();
                try {
                    applicationClientOperationResponse = service.
                            processDataOperation(controller, applicationClientOperationRequest);
                    if(!applicationClientOperationResponse.getOperationSuccessful()){
                        throw new RuntimeException("Aborting transaction");
                    }
                } catch (ServiceException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
				
                resultString = applicationClientOperationResponse.getResultString();
                resultRows = resultString.split(Constants.RESULT_ROW_DELIMITER);
                resultTupleColumn = resultRows[0].split(Constants.
                        RESULT_COLUMN_DELIMITER);
                
				if (resultString.equals(""))
					throw new RuntimeException("I_ID=" + ol_i_id
							+ " not found!");
				s_quantity = Integer.parseInt(resultTupleColumn[0]);
				s_data = resultTupleColumn[1];
				s_dist_01 = resultTupleColumn[2];
				s_dist_02 = resultTupleColumn[3];
				s_dist_03 = resultTupleColumn[4];
				s_dist_04 = resultTupleColumn[5];
				s_dist_05 = resultTupleColumn[6];
				s_dist_06 = resultTupleColumn[7];
				s_dist_07 = resultTupleColumn[8];
				s_dist_08 = resultTupleColumn[9];
				s_dist_09 = resultTupleColumn[10];
				s_dist_10 = resultTupleColumn[11];

				stockQuantities[ol_number - 1] = s_quantity;

				if (s_quantity - ol_quantity >= 10) {
					s_quantity -= ol_quantity;
				} else {
					s_quantity += -ol_quantity + 91;
				}

				if (ol_supply_w_id == w_id) {
					s_remote_cnt_increment = 0;
				} else {
					s_remote_cnt_increment = 1;
				}


				stmtUpdateStock.setInt(1, s_quantity);
				stmtUpdateStock.setInt(2, ol_quantity);
				stmtUpdateStock.setInt(3, s_remote_cnt_increment);
				stmtUpdateStock.setInt(4, ol_i_id);
				stmtUpdateStock.setInt(5, ol_supply_w_id);
				stmtUpdateStock.addBatch();

				ol_amount = ol_quantity * i_price;
				orderLineAmounts[ol_number - 1] = ol_amount;
				total_amount += ol_amount;

				if (i_data.indexOf("GENERIC") != -1
						&& s_data.indexOf("GENERIC") != -1) {
					brandGeneric[ol_number - 1] = 'B';
				} else {
					brandGeneric[ol_number - 1] = 'G';
				}

				switch ((int) d_id) {
				case 1:
					ol_dist_info = s_dist_01;
					break;
				case 2:
					ol_dist_info = s_dist_02;
					break;
				case 3:
					ol_dist_info = s_dist_03;
					break;
				case 4:
					ol_dist_info = s_dist_04;
					break;
				case 5:
					ol_dist_info = s_dist_05;
					break;
				case 6:
					ol_dist_info = s_dist_06;
					break;
				case 7:
					ol_dist_info = s_dist_07;
					break;
				case 8:
					ol_dist_info = s_dist_08;
					break;
				case 9:
					ol_dist_info = s_dist_09;
					break;
				case 10:
					ol_dist_info = s_dist_10;
					break;
				}

				stmtInsertOrderLine.setInt(1, o_id);
				stmtInsertOrderLine.setInt(2, d_id);
				stmtInsertOrderLine.setInt(3, w_id);
				stmtInsertOrderLine.setInt(4, ol_number);
				stmtInsertOrderLine.setInt(5, ol_i_id);
				stmtInsertOrderLine.setInt(6, ol_supply_w_id);
				stmtInsertOrderLine.setInt(7, ol_quantity);
				stmtInsertOrderLine.setFloat(8, ol_amount);
				stmtInsertOrderLine.setString(9, ol_dist_info);
				
				//sqlString = ((JDBC4PreparedStatement)stmtInsertOrderLine).getPreparedSql();
				sqlString = stmtInsertOrderLine.toString();
	            sqlString = sqlString.substring(sqlString.indexOf(":")+2);
				operation = "transactional "+ transactionId + " " + sqlString;
				applicationClientOperationRequest = ApplicationClientRequest.
                        newBuilder().setDataOperation(operation).build();
                try {
                    applicationClientOperationResponse = service.
                            processDataOperation(controller, applicationClientOperationRequest);
                    if(!applicationClientOperationResponse.getOperationSuccessful()){
                        throw new RuntimeException("Aborting transaction");
                    }
                } catch (ServiceException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                //sqlString = ((JDBC4PreparedStatement)stmtUpdateStock).getPreparedSql();
                sqlString = stmtUpdateStock.toString();
                sqlString = sqlString.substring(sqlString.indexOf(":")+2);
                operation = "transactional "+ transactionId + " " + sqlString;
                applicationClientOperationRequest = ApplicationClientRequest.
                        newBuilder().setDataOperation(operation).build();
                try {
                    applicationClientOperationResponse = service.
                            processDataOperation(controller, applicationClientOperationRequest);
                    if(!applicationClientOperationResponse.getOperationSuccessful()){
                        throw new RuntimeException("Aborting transaction");
                    }
                } catch (ServiceException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                
                stmtInsertOrderLine.addBatch();

			} // end-for
			
			//stmtInsertOrderLine.executeBatch();
			//stmtUpdateStock.executeBatch();
            
			total_amount *= (1 + w_tax + d_tax) * (1 - c_discount);
		} catch(UserAbortException userEx)
		{
		    LOG.debug("Caught an expected error in New Order");
		    throw userEx;
		}
	    finally {
            if (stmtInsertOrderLine != null)
                stmtInsertOrderLine.clearBatch();
              if (stmtUpdateStock != null)
                stmtUpdateStock.clearBatch();
        }
		
		String commitRequest = "transactional "+ transactionId+ " Commit";
        ApplicationClientRequest applicationClientCommitRequest = ApplicationClientRequest.newBuilder().
                setDataOperation(commitRequest).build();
        
        ApplicationClientResponse applicationClientCommitResponse = null;
        Boolean didCommit = false;
        try {
            applicationClientCommitResponse = service.processDataOperation(controller, 
                    applicationClientCommitRequest);
            didCommit = applicationClientCommitResponse.getOperationSuccessful();
        } catch (ServiceException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        /* 
        if(didCommit) {
            System.out.println("Commit Successful");
        }
        */
        return didCommit;

	}

}
