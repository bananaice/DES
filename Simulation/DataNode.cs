using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Configuration;
using System.Threading;
using LogService;

namespace Simulation
{
    class DataNode
    {
        private string homepath;
        private uint nodeID;

        private delegate bool AsyncDelegate();

        private Dictionary<string, string> dndc;
        private Queue<DNPar> datanode_queue;

        private List<DataNode> dn_dn_lst = null;
        private NameNode my_nn = null;

        private struct DNPar
        {
            public string srcpath;
            public uint order;
            public int[] pipeline;
            public int replicaID;
            public bool bACK;
        }
     //   private List<Dictionary<string, string>> dn_meta_lst;
        public DataNode(uint assignedID, NameNode nn)
        {
            //dn_meta_lst = new List<Dictionary<string, string>>();
            nodeID = assignedID;
            my_nn = nn; //Get the instance of the name node;
            dndc = new Dictionary<string, string>();
            string str = "Datanode #" + nodeID.ToString() + " has been created.";
            datanode_queue = new Queue<DNPar>();
            //Spawn another thread for every data node to monitor the incoming request of writing.
            Thread th = new Thread(new ThreadStart(MonitorRequest));
            th.Start();

            LogService.LogService.WriteDebug(str);
        }

        public string Homepath
        {
            get
            {
                return homepath;
            }
            set
            {
                this.homepath = value;
            }
        
        }
        public uint NodeID
        {
            get { return nodeID; }
            set { this.nodeID = value; }
        }

        public string Readfile(string filename, uint chunkID)
        {
            string retString = string.Empty;
            List<string> test = new List<string>(dndc.Keys); // chunkIDs
            //TODO: add file name:
            for(int cnt = 0; cnt < dndc.Count; cnt++)
            {
                if (chunkID.ToString() == test[cnt])
                {
                    retString = dndc[test[cnt]];
                }
            }

            return retString;
        }

        public void WriteFile(string srcpath, uint order, int[] pass_pipeline, int ReplicaID, bool ack)
        {
            
        // Get sync'ed with the name node for the data node list
            dn_dn_lst = my_nn.nn_dn_lst;

           DNPar dnpar;
           dnpar.srcpath = srcpath;
           dnpar.order = order;
           dnpar.pipeline = pass_pipeline;
           dnpar.replicaID = ReplicaID;
           dnpar.bACK = ack;

           
           lock(this.datanode_queue)
           {
               datanode_queue.Enqueue(dnpar);

               string logstr = string.Empty;
               logstr += "RIO"; //2 event type
               logstr += "#";
               logstr += "DNEQ"; // 3 event stage  DNEQ = datanode enqued
               logstr += "#";
               logstr += "Enqued to data node: " + this.nodeID.ToString() ; //4 text
               logstr += "#";
               logstr += srcpath + "%" + ReplicaID.ToString() + "%" + order.ToString(); //5 task ID - file name + replica ID + chunkID
               logstr += "#";
               logstr += order.ToString(); //6 chunk ID
               logstr += "#";
               logstr += nodeID.ToString(); //7 data node ID
               LogService.LogService.WriteLog(logstr);
           }
            
            //Done after enque. Let the queue reading process determine when to operate a real write via peeking or dequing. 
            /*
            AsyncDelegate dlgt = new AsyncDelegate(this.RealWriteFile);
            IAsyncResult ar = dlgt.BeginInvoke(null, null);

            
            while (ar.IsCompleted == false)
            {
                Thread.Sleep(1); //Wait
                LogService.WriteDebug("Waiting for the thread to complete");
            }
            bool ret = dlgt.EndInvoke(ar);
            if (!ret)
            {
                //LogService.WriteDebug("No file in the queue");

            }
             */
           // Thread.Sleep(10);
             
            
        
        }

        private void MonitorRequest()
        {
            AsyncDelegate dlgt = new AsyncDelegate(this.RealWriteFile);
            while(true)
            {
                if (datanode_queue.Count == 0)
                {
                    Thread.Sleep(2); //Check the queue every 2 milliseconds.
                    continue;
                }
                else
                {
                    //Leave the peek and deque operations in the RealWriteFile() method.
                    IAsyncResult ar = dlgt.BeginInvoke(null, null);

                    while (ar.IsCompleted == false)
                    {
                        Thread.Sleep(1); //Wait
                        LogService.LogService.WriteDebug("Waiting for the thread to complete");
                    }
                    bool ret = dlgt.EndInvoke(ar);
                    if (!ret)
                    {
                        //LogService.WriteDebug("No file in the queue");

                    }
                    else // Write completed
                    {
                        
                    }
                
                }

                
                
            }
            
            
        }

        //Basic thoughts: maintain a queue at each of the data nodes
        //And the deque operation is conducted by a separate thread.

       //private bool RealWriteFile(string srcpath, uint order, uint replicaID)
       private bool RealWriteFile()
        {
            string source = string.Empty;
            uint order = 0;
            int replicaID = 0;
            int outgoing_replicaID = 0;
            int[] passed_pipeline;
            bool bACKOnly = false;
            bool bOutgoingACK = false;
            bool bDiscontinue = false;

            FileStream fsRead = null;
            FileStream fsWrite = null;


            lock (this.datanode_queue)
            {
                DNPar dnpar;
                if (datanode_queue.Count != 0)
                {
                    dnpar = datanode_queue.Peek();
                    source = dnpar.srcpath;
                    order = dnpar.order;
                    passed_pipeline = dnpar.pipeline;
                    replicaID = dnpar.replicaID;
                    bACKOnly = dnpar.bACK;

                    //DNDQ - DNEQ = waiting time at each data node. this is node specific.

                    string logstr = string.Empty;
                    logstr += "RIO"; //2 event type
                    logstr += "#";
                    logstr += "DNDQ"; // 3 event stage  DNDQ = datanode dequed
                    logstr += "#";
                    logstr += "Enqued to data node: " + this.nodeID.ToString(); //4 text
                    logstr += "#";
                    logstr += source + "%" + replicaID.ToString() + "%" + order.ToString(); //5 task ID - file name + replica ID + chunkID
                    logstr += "#";
                    logstr += order.ToString(); //6 chunk ID
                    logstr += "#";
                    logstr += nodeID.ToString(); //7 data node ID
                    LogService.LogService.WriteLog(logstr);
                }
                else
                {
                    LogService.LogService.WriteDebug("Data node queue is empty !");
                    return false;
                }

            }


            string target = homepath;

            int filesize = Int32.Parse(ConfigurationManager.AppSettings["filesize"]);
            uint numofreplica = (uint)Int32.Parse(ConfigurationManager.AppSettings["replicanum"]);

            string suffix = DateTime.Now.Hour.ToString() + DateTime.Now.Minute.ToString() + DateTime.Now.Second.ToString() + DateTime.Now.Millisecond.ToString();
            target = target + "\\test" + suffix + ".zip";

            //The real full target path
            string chunkid = order.ToString();
            try
            {
                //First of all, the pipeline will be setup so connections will be maintained between client and three data nodes.
                //In real case, all chunks will be splitted into 64KB packets. 
                //Here we will calculate the number of packets and build an appropriate model for the packet transmission
                //string newsource = source + order;

                int int_nodenum = 0; //Determine the next operation as follows,
                // If the current node is primary then pass along the replica to secondary 
                // If the current node is secondary then pass along the replica to tertiary
                // If the current node is tertiary, then pass along ACK to secondary
                // If the current node is secondary and it is an ACK only request, then pass along ACK to primary
                // If the current node is primary and it is an ACK only request, then pass along ACK to client
               
                if (replicaID == 0 && bACKOnly == false)
                {
                    int_nodenum = passed_pipeline[1]; //Set to the secondary node
                    bOutgoingACK = false;
                    outgoing_replicaID = 1;
                }
                else if (replicaID == 1 && bACKOnly == false)
                {
                    int_nodenum = passed_pipeline[2]; ; //set to the tertiary node
                    bOutgoingACK = false;
                    outgoing_replicaID = 2;
                }
                else if (replicaID == 2 && bACKOnly == false)
                {
                    int_nodenum = passed_pipeline[1]; //Set to ACK to secondary
                    bOutgoingACK = true;
                    outgoing_replicaID = 1;

                }
                else if (replicaID == 1 && bACKOnly == true)
                {
                    int_nodenum = passed_pipeline[0]; //Set to ACK to primary
                    bOutgoingACK = true;
                    outgoing_replicaID = 0;
                }
                else if (replicaID == 0 && bACKOnly == true)
                {
                    //Send ACK back to the client.
                    //TODO 31122014 - Add the code for client to receive final ACK so one chunk IO is complete

                    //Currently do nothing and just record the log record
                    //LogService.LogService.WriteLog("ACK received for : " + order.ToString() + " of" + source);
                    /*
                    string logstr = string.Empty;
                    logstr += "RIO"; //2 event type
                    logstr += "#";
                    logstr += "FN"; // 3 event stage
                    logstr += "#";
                    logstr += "ACK received for : " + order.ToString() + " of " + source; //4 text
                    logstr += "#";
                    logstr += source + "%" + replicaID.ToString() + "%" + order.ToString(); //5 task ID - file name + replica ID + chunkID
                    logstr += "#";
                    logstr += chunkid.ToString(); //6 chunk ID
                    logstr += "#";
                    logstr += nodeID.ToString(); //7 data node ID
                    LogService.LogService.WriteLog(logstr);
                     */
                    
                    bDiscontinue = true;
                }

                if (!bDiscontinue)
                {

                    DataNode dn = (DataNode)dn_dn_lst[int_nodenum];

                    if (dn != null)
                    {

                        if (dn.Homepath != null)
                        {
                            //pass along the pipeline list to the primary node
                            dn.WriteFile(source, order, passed_pipeline, outgoing_replicaID, bOutgoingACK);
                        }


                        //Valid data node

                        string temp = System.Environment.CurrentDirectory + "\\DN" + int_nodenum;
                        dn.Homepath = temp;


                    }

                }
                //Here we need to simulate the performance of real IO
                //A typical server SSD can reach IOPS of 50K
                //A typical desktop SSD can reach IOPS of 10K
                //SAS interface can reach the speed of 6Gb per second

                //Look up table:
                //Chunk size and the IO overhead for per chunk on SSD:
                //the latency carried by flash memory is in general: read - 25us; write - 200us; erase-1.5ms
                //Use 64KB as one unit
                //Theoretically 64KB = 64 * 8 = 512kb ; 6Gb/s -> 6Mb / ms
                
                //64KB -> Latency of 2ms
                //When the data is doubled, make the latency x1.5
                
                
                int chunksize = Int32.Parse(ConfigurationManager.AppSettings["filesize"]);
                int times = chunksize / 65536;
                double lat_times = times * 0.75; //x1.5 for every doubled size
                const int unit_lat = 2;
                int act_lat = (int)(unit_lat * lat_times);

                Thread.Sleep(act_lat);

                //After SSD writing is completed
                string logstr = string.Empty;
                logstr += "RIO"; //2 event type
                logstr += "#";
                logstr += "SSDW"; // 3 event stage  SSDW = SSD writing completion
                logstr += "#";
                logstr += "Enqued to data node: " + this.nodeID.ToString(); //4 text
                logstr += "#";
                logstr += source + "%" + replicaID.ToString() + "%" + order.ToString(); //5 task ID - file name + replica ID + chunkID
                logstr += "#";
                logstr += order.ToString(); //6 chunk ID
                logstr += "#";
                logstr += nodeID.ToString(); //7 data node ID
                LogService.LogService.WriteLog(logstr);


                //This is the code for write IO

                /*
                if (File.Exists(source))
                {
                    fsRead = new FileStream(source, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                    fsWrite = new FileStream(target, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);

                    byte[] byts = new byte[fsRead.Length];

                    while (true)
                    {

                        int r = fsRead.Read(byts, 0, byts.Length);
                        if (r <= 0)
                        {

                            break;

                        }
                        fsWrite.Write(byts, 0, byts.Length);

                    }
                }
                else
                {
                    LogService.LogService.WriteLog("File " + source + "does not exist !");
                    return false;

                }

                 */
                
                        
                       // fsWrite.Close();

                    
                    ///fsRead.Close();

               
            }
            catch (Exception ex)
            {
                LogService.LogService.WriteLog("Write IO error" + ex);
                return false;
            }
            finally
            {
                if(fsWrite != null)
                { 
                    fsWrite.Close(); 
                }
                if(fsRead != null)
                {
                    fsRead.Close();
                }
                
            }
           
            numofreplica--; //Get the index number of replica number
            if (numofreplica == replicaID)
            {
                if (File.Exists(source))
                {
                    File.Delete(source);
                }
            }
            chunkid = order.ToString();
            if (replicaID == 0 && bACKOnly == false) //Primary only and data transmission only
            {
                //dndc.Add(chunkid, target);
            }
            if(replicaID == 0 && bACKOnly ==  true) // ACK received from secondary node
            { 
                //LogService.LogService.WriteLog("DataNode " + nodeID.ToString() + " has just finished writing a chunk, with the ID of " + chunkid.ToString() + " to target " + target);
                string logstr = string.Empty;
                logstr += "RIO"; //2 event type
                logstr += "#";
                logstr += "FN"; // 3 event stage
                logstr += "#";
                logstr += "Finish writing a chunk. ACK received"; //4 text
                logstr += "#";
                logstr += source + "%" + replicaID.ToString() + "%" + order.ToString(); //5 task ID - file name + replica ID + chunkID
                logstr += "#";
                logstr += chunkid.ToString(); //6 chunk ID
               logstr += "#";
               logstr += nodeID.ToString(); //7 data node ID
                LogService.LogService.WriteLog(logstr);

            }

            lock (this.datanode_queue)
            {
                datanode_queue.Dequeue();
            }

            return true;

           
        }
    }
}
