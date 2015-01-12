using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Configuration;
using System.Xml;
using LogService;
using System.Collections;


namespace Simulation
{
    class NameNode
    {
        private List<MetaDataInMem> metaDataList = new List<MetaDataInMem>();
        private string targetfilepath = null;
        private static int filesize;
        private static string xmlpath = null;
        private static ArrayList hashlist = new ArrayList();

        private static bool bDedup = false;

        //Replace with key / value ??
        public uint[] GetAddress(string filepath)
        {
            targetfilepath = filepath;
            uint[] datanodelist = null;
            
            //Read info from meta data memory 
            foreach (object obj in metaDataList)
            {
                MetaDataInMem mdim = (MetaDataInMem)obj;
                if (mdim.FileName == filepath) //found the meta data
                {
                    Dictionary<uint, uint> dc = mdim.GetMeta(0);  //The primary copy
                    datanodelist = new uint[dc.Count];
                    //Retrieve the data node lists by order                     
                    for (uint i = 0; i < dc.Count; i++)
                    {
                        datanodelist[i] = dc[i];
                    }
                }
                //Do nothing if not matching

            }

            return datanodelist;
        
        }
        
        /*
        private void AddMeta(Dictionary<uint,uint> dc, string filepath)
        {
            MetaDataInMem mdim = new MetaDataInMem();
            mdim.FileName = filepath;
            //mdim.AddMeta(dc);
            metaDataList.Add(mdim);
        }
         */

        public NameNode(string filepath)
        { 
            //Initialise the name node by reading from the name node meta data file
            //metaDataList = new List<MetaDataInMem>();

            filesize = Int32.Parse(ConfigurationManager.AppSettings["filesize"]);
            xmlpath = System.Environment.CurrentDirectory + ConfigurationManager.AppSettings["metaxmlpath"];
            //TODO construct the meta data file in persistent memory

            if (ConfigurationManager.AppSettings["dedup"] == "on")
            {
                bDedup = true;
            }

            //hashlist = new ArrayList();
            //Read from persisitent file


            //Load the hash value files
            //26/11: Hardcode for testing purpose. TODO: remove the hardcoded path. Add try/catch for IO operation
           


        }

        public List<DataNode> nn_dn_lst = null;
        public List<Dictionary<uint, uint>> WriteRequest(string filename)
        {

            //This is from HDFS source file comments:
            /**
            * The class is responsible for choosing the desired number of targets
            * for placing block replicas.
            * The replica placement strategy is that if the writer is on a datanode,
            * the 1st replica is placed on the local machine, 
            * otherwise a random datanode. The 2nd replica is placed on a datanode
            * that is on a different rack. The 3rd replica is placed on a datanode
            * which is on a different node of the rack as the second replica.
            */

            try
            {
                List<Dictionary<uint, uint>> lst = new List<Dictionary<uint, uint>>();
                Dictionary<uint, uint> retVal = new Dictionary<uint, uint>();
                MetaDataInMem mdim = new MetaDataInMem();
                mdim.FileName = filename;

                //Calculate the number of the chunks
                FileStream SplitFileStream = new FileStream(filename, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                uint filecount = (uint)(SplitFileStream.Length / filesize);

                if (SplitFileStream.Length % filesize != 0)
                {
                    filecount++;
                }
                SplitFileStream.Close(); //Close the file stream

                //uint dn_id = 0; // Test for putting everything to DN0

                //Randomly select the data node from the total number of datanodes
                int numofdatanode = Int32.Parse(ConfigurationManager.AppSettings["datanodenum"]);
                int numofreplica = Int32.Parse(ConfigurationManager.AppSettings["replicanum"]);
                bool bRackEnabled = bool.Parse(ConfigurationManager.AppSettings["rackenabled"]);

                
                //Otherwise, this section should be the node assigning algorithms
                // 1. benchmark is random if not considering the rack assignment of nodes
                // with no load balancing and availaibility check
                // 2. DHT is used for content address
                XmlDocument xmldoc = new XmlDocument();
                xmldoc.Load(xmlpath);
                XmlNode root = xmldoc.SelectSingleNode("meta");

                XmlElement xe = xmldoc.CreateElement("filemeta");
                xe.SetAttribute("filename", mdim.FileName);
                //Create timestamp here ?
                xe.SetAttribute("numofchunk", filecount.ToString());

                uint dn_id = 0;
                uint temp_value1 = 0;
                uint temp_value2 = 0;
                //Without dedup, generate random list of data node
                if (!bDedup)
                {
                    Random rd = new Random();
                    for (uint i = 0; i < filecount; i++)
                    {
                       
                        
                        if (!bRackEnabled) //Rack turned off
                        {
                            dn_id = (uint)rd.Next(0, numofdatanode - 1);
                        }
                        else //Rack enabled
                        { 
                            //We do not know where the client comes from so pickup the node randomly anyway
                            dn_id = (uint)rd.Next(0, numofdatanode - 1);
                        }
                        mdim.AddMeta(i, dn_id, 0); //Primary copy
                        XmlElement xe2 = xmldoc.CreateElement("chunkmeta");
                        xe2.SetAttribute("ChunkID", i.ToString());
                        xe2.SetAttribute("DatanodeID", dn_id.ToString());


                        if (!bRackEnabled)
                        {
                            dn_id = (uint)rd.Next(0, numofdatanode - 1);
                        }
                        else
                        {
                            //Assuming the probabality of hitting every node is same, then filter all unwanted results
                            temp_value1 = dn_id;
                            do
                            {
                                dn_id = (uint)rd.Next(0, numofdatanode - 1);
                            }
                            while(SameRack(temp_value1, dn_id));
                        }
                        mdim.AddMeta(i, dn_id, 1); //Secondary replica
                        xe2.SetAttribute("SecDatanodeID", dn_id.ToString());

                        if (!bRackEnabled)
                        {
                            dn_id = (uint)rd.Next(0, numofdatanode - 1);
                        }
                        else
                        {
                            //Assuming the probabality of hitting every node is same, then filter all unwanted results
                            temp_value2 = dn_id;
                            do
                            {
                                dn_id = (uint)rd.Next(0, numofdatanode - 1);
                            }
                            while (SameRack(temp_value1, temp_value2, dn_id)) ;
                        }
                        
                        mdim.AddMeta(i, dn_id, 2); //Tertiary replica
                        xe2.SetAttribute("TerDatanodeID", dn_id.ToString());


                        xe.AppendChild(xe2);

                    }
                }
                else // dedup on
                {
                    string tarpath = @"C:\DES\PreProcessor\bin\Debug\chunk\";


                    string[] file_original_name_arr = filename.Split('\\');
                    string hash_name = file_original_name_arr[file_original_name_arr.Length - 1] + "-hash.txt";
                    tarpath += hash_name;
                    

                    StreamReader srReadFile = new StreamReader(tarpath); //Load the hash value file

                    Random rd = new Random();
                    //uint dn_id = 0;
                    XmlElement xe2 = null;
                    for (uint i = 0; i < filecount; i++)
                    {
                        string strReadLine = srReadFile.ReadLine(); //Hash value

                        if (hashlist.Count == 0)
                        {
                            if (!bRackEnabled) //Rack turned off
                            {
                                dn_id = (uint)rd.Next(0, numofdatanode - 1);
                            }
                            else //Rack enabled
                            {
                                //We do not know where the client comes from so pickup the node randomly anyway
                                dn_id = (uint)rd.Next(0, numofdatanode - 1);
                            }
                            mdim.AddMeta(i, dn_id, 0); //Primary copy
                            xe2 = xmldoc.CreateElement("chunkmeta");
                            xe2.SetAttribute("ChunkID", i.ToString());
                            xe2.SetAttribute("DatanodeID", dn_id.ToString());


                           // hashlist.Add(strReadLine); //Add primary node  //TODO - remember the primary node only.
                            xe2.SetAttribute("HashVal", strReadLine);  // Write hash value into xml file

                            if (!bRackEnabled)
                            {
                                dn_id = (uint)rd.Next(0, numofdatanode - 1);
                            }
                            else
                            {
                                //Assuming the probabality of hitting every node is same, then filter all unwanted results
                                temp_value1 = dn_id;
                                do
                                {
                                    dn_id = (uint)rd.Next(0, numofdatanode - 1);
                                }
                                while (SameRack(temp_value1, dn_id)) ;
                            }
                            mdim.AddMeta(i, dn_id, 1); //Secondary replica
                            xe2.SetAttribute("SecDatanodeID", dn_id.ToString());


                            if (!bRackEnabled)
                            {
                                dn_id = (uint)rd.Next(0, numofdatanode - 1);
                            }
                            else
                            {
                                //Assuming the probabality of hitting every node is same, then filter all unwanted results
                                temp_value2 = dn_id;
                                do
                                {
                                    dn_id = (uint)rd.Next(0, numofdatanode - 1);
                                }
                                while (SameRack(temp_value1, temp_value2, dn_id)) ;
                            }
                            mdim.AddMeta(i, dn_id, 2); //Tertiary replica
                            xe2.SetAttribute("TerDatanodeID", dn_id.ToString());

                            hashlist.Add(strReadLine); 
                            xe.AppendChild(xe2);
                            continue;
                        }
                        ArrayList temparr = (ArrayList)hashlist.Clone();
                        bool bDone = false;
                        foreach (Object obj in temparr) //Optimise searching as this is sorted...
                        {

                            if (obj.ToString() == strReadLine) //Increment existing hash value
                            {
                                //hashlist.Add(strReadLine, count);

                                //No new xml entry is added. 
                                //Instead, look up the existing xml file
                                foreach (XmlElement xmlel in root)
                                {
                                    foreach (XmlElement subxmlel in xmlel)
                                    {
                                        if (subxmlel.GetAttribute("HashVal") == strReadLine)
                                        {
                                          

                                            uint node_no = (uint)Int32.Parse(subxmlel.GetAttribute("DatanodeID"));
                                            mdim.AddMeta(i, node_no, 0);
                                            xe2 = xmldoc.CreateElement("chunkmeta");
                                            xe2.SetAttribute("ChunkID", i.ToString());
                                            xe2.SetAttribute("DatanodeID", node_no.ToString());
                                            xe2.SetAttribute("HashVal", strReadLine);  // Write hash value into xml file
                                            node_no = (uint)Int32.Parse(subxmlel.GetAttribute("SecDatanodeID"));
                                            mdim.AddMeta(i, node_no, 1);
                                            xe2.SetAttribute("SecDatanodeID", node_no.ToString());
                                            node_no = (uint)Int32.Parse(subxmlel.GetAttribute("TerDatanodeID"));
                                            mdim.AddMeta(i, node_no, 2);
                                            xe2.SetAttribute("TerDatanodeID", node_no.ToString());
                                            xe.AppendChild(xe2);
                                            bDone = true;

                                        }
                                    }
                                }
                                //Another case is that if the hash appears once in the same file, the need to iterate the 'xe' that has not been committed to the xml root
                                foreach (XmlElement subxmlel2 in xe)
                                {
                                    if(!bDone)
                                    { 
                                        if (subxmlel2.GetAttribute("HashVal") == strReadLine)
                                        { 
                                            uint node_no = (uint)Int32.Parse(subxmlel2.GetAttribute("DatanodeID"));
                                            mdim.AddMeta(i, node_no, 0);
                                            xe2 = xmldoc.CreateElement("chunkmeta");
                                            xe2.SetAttribute("ChunkID", i.ToString());
                                            xe2.SetAttribute("DatanodeID", node_no.ToString());
                                            xe2.SetAttribute("HashVal", strReadLine);  // Write hash value into xml file
                                            node_no = (uint)Int32.Parse(subxmlel2.GetAttribute("SecDatanodeID"));
                                            mdim.AddMeta(i, node_no, 1);
                                            xe2.SetAttribute("SecDatanodeID", node_no.ToString());
                                            node_no = (uint)Int32.Parse(subxmlel2.GetAttribute("TerDatanodeID"));
                                            mdim.AddMeta(i, node_no, 2);
                                            xe2.SetAttribute("TerDatanodeID", node_no.ToString());
                                            xe.AppendChild(xe2);
                                            bDone = true;
                                        }
                                    }
                                }
                                  
                            }
                            else //Add a new entry
                            {
                               //do nothing
                                //hashlist.Add(strReadLine);

                                //Follow the random way
                            }
                            

                        }

                        if (bDone) //If an xml node has been added to match the previous hash value, then continue into next i count for next ordered chunk.
                        {
                            continue;
                        }
                        //NO match is found, will add one new hash value
                        if (!bRackEnabled) //Rack turned off
                        {
                            dn_id = (uint)rd.Next(0, numofdatanode - 1);
                        }
                        else //Rack enabled
                        {
                            //We do not know where the client comes from so pickup the node randomly anyway
                            dn_id = (uint)rd.Next(0, numofdatanode - 1);
                        }
                      
                        mdim.AddMeta(i, dn_id, 0); //Primary copy

                        xe2 = xmldoc.CreateElement("chunkmeta");
                        xe2.SetAttribute("ChunkID", i.ToString());
                        xe2.SetAttribute("DatanodeID", dn_id.ToString());


                        hashlist.Add(strReadLine); 
                        xe2.SetAttribute("HashVal", strReadLine);  // Write hash value into xml file

                        if (!bRackEnabled)
                        {
                            dn_id = (uint)rd.Next(0, numofdatanode - 1);
                        }
                        else
                        {
                            //Assuming the probabality of hitting every node is same, then filter all unwanted results
                            temp_value1 = dn_id;
                            do
                            {
                                dn_id = (uint)rd.Next(0, numofdatanode - 1);
                            }
                            while (SameRack(temp_value1, dn_id)) ;
                        }
                        mdim.AddMeta(i, dn_id, 1); //Secondary replica
                        xe2.SetAttribute("SecDatanodeID", dn_id.ToString());


                        if (!bRackEnabled)
                        {
                            dn_id = (uint)rd.Next(0, numofdatanode - 1);
                        }
                        else
                        {
                            //Assuming the probabality of hitting every node is same, then filter all unwanted results
                            temp_value2 = dn_id;
                            do
                            {
                                dn_id = (uint)rd.Next(0, numofdatanode - 1);
                            }
                            while (SameRack(temp_value1, temp_value2, dn_id)) ;
                        }
                        mdim.AddMeta(i, dn_id, 2); //Tertiary replica
                        xe2.SetAttribute("TerDatanodeID", dn_id.ToString());


                        xe.AppendChild(xe2);

                       

                       

                    }
                    srReadFile.Close();  
                            
                           
                            

                        
                    
                }

                root.AppendChild(xe);
                xmldoc.Save(xmlpath);


                metaDataList.Add(mdim); // Add to the global meta data list

                //Write to the xml persistent as well.

                for (uint ccnt = 0; ccnt < numofreplica; ccnt++)
                {
                    lst.Add(mdim.GetMeta(ccnt)); //Add all replicas
                 }

                return lst; 
            }
            catch (Exception ex)
            {
                Console.WriteLine("FileIO error @WriteRequest." + ex);
                return null;
            }
           
            
        }

        private bool SameRack(uint assignment1, uint assignment2, uint tobeassigned)
        {
            
            uint numofrack = (uint)Int32.Parse(ConfigurationManager.AppSettings["racknum"]);

            uint divident3 = tobeassigned / numofrack;
            uint divident2 = assignment1 / numofrack;
            uint divident1 = assignment2 / numofrack;
            if (divident1 == divident3 || divident2 == divident3) //If tobeassigned falls into the same rack of already assigned , turn true; otherwise false
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        private bool SameRack(uint assignment1, uint tobeassigned)
        {
            bool bRackEnabled = bool.Parse(ConfigurationManager.AppSettings["rackenabled"]);
            uint numofrack = (uint)Int32.Parse(ConfigurationManager.AppSettings["racknum"]);

            uint divident1 = tobeassigned / numofrack;
            uint divident2 = assignment1 / numofrack;
            if (divident1 == divident2) //If tobeassigned falls into the same rack of already assigned , turn true; otherwise false
            {
                return true;
            }
            else
            {
                return false;
            }

        }

        public Dictionary<uint, uint> ReadRequest(string filename)
        { 
            Dictionary<uint,uint> retVal = null;
            //Query the list
            foreach (object obj in metaDataList)
            {
                MetaDataInMem mdim = (MetaDataInMem)obj;
                if (mdim.FileName == filename)
                {
                    retVal = mdim.GetMeta(0); // Primary copy
                    break;
                }
                else
                {
                    continue;
                }
                    
            }
            return retVal;
        }
        private class MetaDataInMem
        {
            private string filename = null;
            public string FileName
            {
                get
                {
                    return this.filename;
                }
                set { this.filename = value; }
                
            }

            private Dictionary<uint,uint> filemeta = new Dictionary<uint,uint>();
            private Dictionary<uint, uint> filemeta_sec = new Dictionary<uint, uint>();
            private Dictionary<uint, uint> filemeta_ter = new Dictionary<uint, uint>();


            public void AddMeta(uint chunkid, uint datanodeid, uint replicaID)
            {
                if (replicaID == 0)
                {
                    filemeta.Add(chunkid, datanodeid);
                }
                else if (replicaID == 1)
                {
                    filemeta_sec.Add(chunkid, datanodeid);
                }
                else if (replicaID == 2)
                {
                    filemeta_ter.Add(chunkid, datanodeid);
                }
                else
                { 
                    //Currently no more than three replicas are supported. 

                }
            }

            

            public Dictionary<uint, uint> GetMeta(uint replicaID)
            {
                if(replicaID == 0)
                {
                    return filemeta;
                }
                else if (replicaID == 1)
                {
                    return filemeta_sec;
                }
                else if (replicaID == 2)
                {
                    return filemeta_ter;
                }
                else
                {
                    //Currently no more than three replicas are supported. 
                    return filemeta;
                }
            }

        
        }
    }
}
