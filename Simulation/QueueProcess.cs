using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Configuration;
using Priority_Queue;
using LogService;

namespace Simulation
{
    public class QueueProcess
    {
        //private static Queue<Task> sQueue = new Queue<Task>();

        private const int MAX_QUEUE_CAPACITY = 200000000; // The library has a limit on capacity but I will see if I can remove it safely. 
        private const string time_delimiter = ":";
        private static HeapPriorityQueue<Task> pQueue = new HeapPriorityQueue<Task>(MAX_QUEUE_CAPACITY);

        public delegate void QueueEventHandler(string eventstr); 
       // public event QueueEventHandler QueueEvent;

        private static NameNode nn = null;
        private static List<DataNode> dn_lst = null;
        private uint datanode_num; //Configurable number.should be read from the configuration file later.

      
        

        public void Enque(Task task)
        {

            
            double timestamp = DateTime.Now.ToOADate(); //Convert the current time into double so that it could be used as priority factor.
            pQueue.Enqueue(task, timestamp);
            QueueChangeMsg(true, task);
             
             
            /*
            
            int queue_cnt = sQueue.Count;
            bool enqueued = false;

            double timestamp = DateTime.Now.ToOADate();
            //Always assign priority before enque into sQueue.
            task.Priority = timestamp;

            lock (sQueue)
            {
                if (queue_cnt == 0)
                {
                    //Directly enque into the empty queue
                    sQueue.Enqueue(task);
                    QueueChangeMsg(true, task);
                }
                else
                {
                    queue_cnt++; //Count the new task as one :)
                }

                while (queue_cnt > 0) //There are still elements that are not compared in the queue
                {
                    if (enqueued == false)
                    {
                        Task temp = sQueue.Peek();
                        if (task.Priority <= temp.Priority) // The new element is earlier. Regard "equal" as earlier here.
                        {
                            sQueue.Enqueue(task); // Really it is an insert to the queue
                            enqueued = true; //Mark it as enqued
                            QueueChangeMsg(true, task);
                            queue_cnt--;
                        }
                        else //The old element is earlier, so deque it and enque again to the end of the queue
                        {
                            temp = sQueue.Dequeue();
                            sQueue.Enqueue(temp);
                            queue_cnt--;

                            //Special condition: if the only left one is the new task, enque it.
                            if (queue_cnt == 1)
                            {
                                sQueue.Enqueue(task);
                                enqueued = true;
                                QueueChangeMsg(true, task);
                                queue_cnt--;
                            }

                        }
                    }
                    else //The new element has been enqued so add all other elements after it. 
                    {
                        Task temp = sQueue.Dequeue();
                        sQueue.Enqueue(temp);
                        queue_cnt--;
                    }
                }

            }
            */
             
        }
        private void QueueChangeMsg(bool bEnque, Task temp)
        {
            //Send notification to the winForm and/or append to log file



            string logstr = string.Empty;

            logstr += temp.TASKSTAGE; //2 event type
            logstr += "#";
            if (bEnque)
            {
                logstr += "EN";
            }
            else
            {
                logstr += "DE";
            }
            // 3 event stage
            logstr += "#";
            logstr += "No text"; //4 text
            logstr += "#";
            logstr += temp.FILEPATH + temp.ORDER.ToString() + "%" + temp.REPLICA.ToString() + "%" + temp.ORDER.ToString(); //5 task ID - file name + replica ID + chunkID
            logstr += "#";
            logstr += temp.ORDER.ToString(); //6 chunk ID
            logstr += "#";
            logstr += temp.NODENUM.ToString(); //7 data node ID

            // logstr += " Source " + order.ToString() + " as replciate No." + replicaID + " has been written to " + temp; //Home path is the destination
            LogService.LogService.WriteLog(logstr);
        }

        public Task Deque()
        {
           
            /*
            lock (sQueue)
            {
                return sQueue.Dequeue();
            }
            */
             
            
            
            lock (pQueue)
            { 
                return (Task)pQueue.Dequeue();
            }
             
             
        }

        public int QueueCount
        {
            get
            {
                //return sQueue.Count;
                return pQueue.Count;
            }
        }

        public void QueueScanning()
        {
            TaskExceution tex = new TaskExceution();
            while(true)
            {
                if (pQueue.Count == 0)
                //if(sQueue.Count == 0)
                { continue; }

                //Task temp = sQueue.Peek();
                Task temp = (Task)pQueue.Peek();
                //Scan the queue and trigger the event when the time is hit
                double timenow = DateTime.Now.ToOADate();

                
                    if (timenow >= temp.Priority) //The scheduled time has arrived.   
                    {
                        
                        lock (pQueue)
                        {
                            temp = pQueue.Dequeue();                           
                            
                        }
                         
                         
                        /*
                        lock (sQueue)
                        {
                            temp = sQueue.Dequeue();

                        }
                        */
                         


                        QueueChangeMsg(false, temp);
                        
                        //QueueEvent(str);
                        //Start to execute the task. Spawn another thread ?
                        //TODO to refactor the single thread code
                        //  TODO to use delegate to optimise the code
                        
                        if (temp.TASKID == "W" && temp.TASKSTAGE == "CNN")
                        {
                            tex.WriteCNN(temp.FILEPATH, nn);
                        }
                        else if (temp.TASKID == "W" && temp.TASKSTAGE == "NNC")
                        {
                            tex.WriteNNC(temp.FILEPATH, nn);
                        }
                        else if (temp.TASKID == "W" && temp.TASKSTAGE == "CDN")
                        {
                            tex.WriteCDN(temp.FILEPATH, (List<Dictionary<uint,uint>>)temp.DNLIST);
                        }
                        else if (temp.TASKID == "W" && temp.TASKSTAGE == "RIO")
                        {
                            tex.WriteRIO(temp.FILEPATH, dn_lst, temp.ORDER, temp.PIPELINE);
                        }
                        else if (temp.TASKID == "R" && temp.TASKSTAGE == "CNN")
                        {
                            tex.ReadCNN(temp.FILEPATH);
                        }
                        else if (temp.TASKID == "R" && temp.TASKSTAGE == "NNC")
                        {
                            tex.ReadNNC(temp.FILEPATH, nn);
                        }
                        else if (temp.TASKID == "R" && temp.TASKSTAGE == "CDN")
                        {

                            tex.ReadCDN(temp.FILEPATH, (Dictionary<uint,uint>)temp.DNLIST, dn_lst);
                        }
                        else if (temp.TASKID == "R" && temp.TASKSTAGE == "RIO")
                        {
                            //tex.ReadIO(dn_lst, temp.ORDER, temp.NODENUM);
                        }
                        else
                        {
                            //other cases
                        }
                    }
            }
        
        }
        public QueueProcess()
        { 
            //sQueue = new Queue<Task>();

            

            //Initialise the name node here.
             nn = new NameNode(null);

             string currentpath = System.Environment.CurrentDirectory;

             datanode_num = (uint)Int32.Parse(ConfigurationManager.AppSettings["datanodenum"]);
            //Create data node dynamically and add them to the container
            //Later, this list is managed by Name Node, if there are new nodes joining or departing.
             dn_lst = new List<DataNode>();
            for (uint i = 0; i < datanode_num; i++)
            {
                DataNode dn = new DataNode(i, nn);
                //dn.NodeID = i;
                dn_lst.Add(dn);
                //And also create the repository directories for data nodes
                /*
                string curnodepath = currentpath + "\\DN" + i.ToString();
                if(Directory.Exists(curnodepath) == false)
                {
                    Directory.CreateDirectory(curnodepath);
                }
                */
                //No need to create data node temp folders for now.
            }
            nn.nn_dn_lst = dn_lst;
        }

        


    }

    public class Task : PriorityQueueNode
    // public class Task 
    {
        // private DateTime scheduled;
        private string TaskID;  // TaskID = R - Read | W - Write | D - Delete
        private string TaskStage; // CNN - Client to NameNode | NNC - Name Node to Client | CDN - Client and Data Node
        private string Filepath; 
        private Object DNList;
        private uint Order;
        private uint Nodenum;
        private uint Replica;
        //private double priority;
        private int[] Pipeline;
        public Task(string taskID, string taskStage, string filepath, Object obj, uint order, int[] pipeline)
        //public Task(string taskID, string taskStage, string filepath, Object obj, uint order, uint nodenum, uint replica)
        {
            this.TaskID = taskID;
            this.TaskStage = taskStage;
            //this.scheduled = sch;
            this.Filepath = filepath;
            this.DNList = obj;
            this.Order = order;
            //this.Nodenum = nodenum;
            this.Pipeline = pipeline;
            //this.Replica = replica;
        }
        
        public string TASKID
        {
            get
            {
                return TaskID;
            }
        }
        /*
        public double Priority
        {
            get
            {
                return priority;
            }
            set
            {
                priority = value;
            }
        }
         */
        public string TASKSTAGE
        {
            get
            {
                return TaskStage;
            }
        }

        public string FILEPATH
        {
            get
            {
                return Filepath;
            }
        }

        public Object DNLIST
        {
            get
            {
                return DNList;
            }
        }

        public uint ORDER
        {
            get
            {
                return Order;
            }
        }
        public uint NODENUM
        {
            get
            {
                return Nodenum;
            }
        
        }
        public int[] PIPELINE
        {
            get
            {
                return Pipeline;
            }
            set
            {
                this.PIPELINE = value;
            }
        }
        public uint REPLICA
        {
            get
            {
                return Replica;
            }
            set
            {
                this.Replica = value;
            }
        }

    }
}
