using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.IO;
using LogService;
using System.Configuration;

namespace Simulation
{
    class TaskExceution
    {
        private QueueProcess qp = new QueueProcess();

        //this function sends the write request from client to the name node to generate the data node list
        //This is the start of a write operation of a file
        public bool WriteCNN(string filepath, NameNode nn)
        {
            //Request the NameNode to return the metadata
            //int os = 0; //offset - basically network delay for TCP communication between client and name node
            Task task = new Task("W", "NNC", filepath, null, 0, null);
            qp.Enque(task); //TODO: exception handling
            return true;

            
        }

        //This function gets the list of data node from the name node including the replica settings.
        public bool WriteNNC(string filepath, NameNode nn)
        {
            //Return the meta data 
            List<Dictionary<uint, uint>> lst = null;
            //Dictionary<uint, uint> dc = null;
            //int os = 0; //offset - basically network delay for TCP communication between client and name node

            lst = nn.WriteRequest(filepath);

            Task task = new Task("W", "CDN", filepath, lst, 0, null);
            qp.Enque(task); //TODO: exception handling
           

            
            
            return true;
        }

        //This functions pushes the real IO job into the queue. One job per chunk.
        //Chunk size is variable. Packet size is 64KB
        //In this setup chunk size is at least equal to packet size, which is 64KB
        //If chunk size is greater than packet size, then still, one job per chunk. Within each job, there will be multiple packets to transfer
        public void WriteCDN(string source, List<Dictionary<uint,uint>> lst)
        {


            //The new design should be:
            //1-Name node returns the list of three replica data node numbers to client
            //2-Client set up the pipeline. The client should also send the "order" of replica data nodes to the 1st data node and it will cascade and pass along.
            //3-Client send data packets to the 1st data node. The client should also send the "order" of replica data nodes to the 1st data node and it will cascade and pass along.
            //   
            //   
            //4-The 1st data node sends the packets to the 2nd data node
            //5-The 2nd data node sends the packets to the 3rd data node
            //6-The 3rd data node sends the ACK to the 2nd data node
            //7-The 2nd data node sends the ACK to the 1st data node
            //8-The 1st data node sends the ACK to the client
            //9-A write transaction is completed.

            

            //int os = 1;
            
            //Chunking has been done prior
            //Here push all write events into the queue for RIO to execute.

            int numofreplicas = Int32.Parse(ConfigurationManager.AppSettings["replicanum"]);

            if (lst == null)
            {
                string logstr = string.Empty;
                logstr += "Name node does not return the correct number of data node lists.";
                LogService.LogService.WriteLog(logstr);
                return;
            }

            if (lst.Count != numofreplicas)
            {
                string logstr = string.Empty;
                logstr += "Name node does not return the correct number of data node lists.";
                LogService.LogService.WriteLog(logstr);
                return;
            }

            //Primary, Secondary and Tertiary node lists
            Dictionary<uint, uint> dc_primary = lst[0];
            Dictionary<uint, uint> dc_secondary = lst[1];
            Dictionary<uint, uint> dc_tertiary = lst[2];

            //Set up the pipeline for each chunk and store the pipeline info into an array
            int[] pipeline = new int[numofreplicas];

            for (uint cnt = 0; cnt < dc_primary.Values.Count; cnt++)
            {
                pipeline[0] = (int)dc_primary[cnt];
                pipeline[1] = (int)dc_secondary[cnt];
                pipeline[2] = (int)dc_tertiary[cnt];
                //Operation / stage / source / obj / order of the chunk / node list(pipeline) 
                Task task = new Task("W", "RIO", source, null, cnt, pipeline);
                qp.Enque(task);
            
            }

            
        }

        //Used for real IO operation - Write the chunks to the data nodes.
        public void WriteRIO(string source, List<DataNode> lst, uint order, int[] pipeline)
        {
            //'Source + order' becomes an unique identifier of data segment.
            //The data node pipeline info should be passed along to secondary and tertiary data nodes by the primary node


            string newsource = source + order;

            int int_nodenum = pipeline[0]; //primary node
            DataNode dn = (DataNode)lst[int_nodenum];
            
            if (dn != null)
            {
                
                if (dn.Homepath != null)
                {
                    //pass along the pipeline list to the primary node, as a data transmission req
                    dn.WriteFile(newsource, order, pipeline, 0, false);
                }
                 

                //Valid data node

                string temp = System.Environment.CurrentDirectory + "\\DN" + int_nodenum;
                dn.Homepath = temp;

                               
            }
            
            
        }
        public bool ReadCNN(string filename)
        {
            //Request the NameNode to return the metadata
            //int os = 0; //offset - basically network delay for TCP communication between client and name node
            Task task = new Task("R", "NNC", filename, null, 0, null);
            qp.Enque(task); //TODO: exception handling
            return true;
        }
        public bool ReadNNC(string filename, NameNode nn)
        {
            //int os = 0; //offset - basically network delay for TCP communication between client and name node
            Dictionary<uint, uint> dc = nn.ReadRequest(filename); //return the meta data from the name node
            Task task = new Task("R", "CDN", filename, dc, 0, null);
            qp.Enque(task); //TODO: exception handling
            return true;
        }

        public void ReadCDN(string filepath, Dictionary<uint, uint> dc, List<DataNode> lst)
        {

           // int os = 10;
            uint nodenumber = 0;

           // filepath = @"C:\11.MyResearch\AUG2014\DES\MyDES\DES\DES\bin\Debug\Client\Test2.zip";

            FileStream AddStream = new FileStream(filepath, FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite); 
            BinaryWriter AddWriter = new BinaryWriter(AddStream);

            for (uint cnt = 0; cnt < dc.Values.Count; cnt++)
            {
                nodenumber = dc[cnt];
                //DataNode dn = new DataNode();
                //Rewrite this to make it more efficient. Optimisation
                int int_nodenum = (int)nodenumber;
                DataNode dn = (DataNode)lst[int_nodenum];

                string addpath = dn.Readfile(null, cnt);

                FileStream TempStreamA = new FileStream(addpath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                BinaryReader TempReaderA = new BinaryReader(TempStreamA);

                AddWriter.Write(TempReaderA.ReadBytes((int)TempStreamA.Length));
                TempReaderA.Close();
                TempStreamA.Close();    

            }

            AddWriter.Close();
            AddStream.Close();

            string logstr = string.Empty;
            logstr += " File " + filepath + " has been read.";
            LogService.LogService.WriteLog(logstr);
        }



        public void ReadIO(List<DataNode> lst, uint cnt, uint nodenum)
        {
            //DataNode dn = new DataNode();
            //Rewrite this to make it more efficient. Optimisation
            int int_nodenum = (int)nodenum;
            DataNode dn = (DataNode)lst[int_nodenum];

            string filepath = dn.Readfile(null, cnt);

            
        }

    }
}
