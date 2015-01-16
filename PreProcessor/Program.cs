using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using LogService;
using System.IO;
using System.Collections;
using System.Diagnostics;
using System.Security;
using System.Xml;

namespace PreProcessor
{
    class Program
    {
        static void Main(string[] args)
        {
            //Pre-process all files by chunking them into chunks as well as saving to different destinations for fetch use later
            //At the same time, set up a switch to turn the hash value calculation on and off. It may also change the selection of hashing algorithms.

            //Read the input file. 
            //For all 'W' requests, chunk the file and get the hash value ready
            //For all 'R' requests, chunk the file and store them to triply replicated locations. 
            //The xml file used as persistent storage will be updated and Name Node (NN) reloads it upon startup.
            //Clear log file

            string log_time_identifier = "#";


            //LogService.LogService ls = new LogService.LogService();
            LogService.LogService.ClearLog();
            
            if (args[0] == "copy")  //Copy all files in sub directories to one directory recursively. If names conflict, then overwrite the existing ones.
            { 
                //Copy files into the directory
                FileListGen(args[1], args[2]);
                return;
            }

            if (args[0] == "chunk")
            {
                string repo_path = string.Empty;
                string dest_path = string.Empty;
                repo_path = args[1];
                dest_path = args[2];
                FileChunker fc = new FileChunker(repo_path, dest_path);
                fc.BuildBatchChunk();
                return;
            }

            if (args[0] == "dup")
            {
                string tar_path = args[1];
                CalcHashDup(tar_path);
                return;
            }

            if (args[0] == "log")
            {
                //Log analysis
                string logpath = args[1];
                LogAnalysis(logpath);
                return;
            }

            if (args[0] == "xml")
            {
                //Parse xml file to see the data node distribution
                string xmlpath = args[1];
                DataNodeDistr(xmlpath);
                return;
            }

            //qp.QueueEvent += RefreshRichTextBox;
            string filepath = System.Environment.CurrentDirectory + "\\DES.txt";
            if (filepath != null)
            {
                //Read the configuration file
                StreamReader srReadFile = new StreamReader(filepath);


                while (!srReadFile.EndOfStream)
                {
                    string strReadLine = srReadFile.ReadLine();
                    string[] arr = strReadLine.Split(';');
                    int offset = 0;
                    if (arr[1] != null)
                    {
                        offset = Int16.Parse(arr[1]);

                    }
                    //DateTime sch = DateTime.Now;

                    //LogService.LogService.WriteLog("-Now time is :" + log_time_identifier + DateTime.Now.Minute.ToString() + ":" + DateTime.Now.Second.ToString() + ":" + DateTime.Now.Millisecond.ToString());

                    
                    string taskpath = System.Environment.CurrentDirectory + "\\Client\\" + arr[0];
                    /*
                    if (arr[2] == "W")
                    {
                        Task mytask = new Task("W", "CNN", taskpath, null, 0, 0, 0);
                        qp.Enque(mytask);
                    }
                    else if (arr[2] == "R")
                    {
                        Task mytask = new Task("R", "CNN", taskpath, null, 0, 0, 0);
                        qp.Enque(mytask);
                    }
                     */

                    //Instead of queueing, take the actions right away...
                    if (arr[2] == "W") //Write request
                    {
                        FileChunker fc = new FileChunker(taskpath, null);
                        fc.BuildChunk();
                        LogService.LogService.WriteLog("-Chunks are built. " + log_time_identifier + DateTime.Now.Minute.ToString() + ":" + DateTime.Now.Second.ToString() + ":" + DateTime.Now.Millisecond.ToString());
                            
                    }
                    else if (arr[2] == "R")
                    { 
                        //Implement later for read.
                    }
                }


                // Close the stream
                srReadFile.Close();

                

            }
        }



        private static void GetAll(DirectoryInfo dir, ArrayList FileList)//Traverse all files.
        {
            //ArrayList FileList = new ArrayList();

            FileInfo[] allFile = dir.GetFiles();
            foreach (FileInfo fi in allFile)
            {
                //Only add bigger files with size >= 16KB
                if(fi.Length >= 16384)
                { 
                    FileList.Add(fi.FullName);
                }
            }

            DirectoryInfo[] allDir = dir.GetDirectories();
            foreach (DirectoryInfo d in allDir)
            {
                GetAll(d, FileList);
            }
            //return FileList;
        }
        private static void FileListGen(string src, string target)
        {

            DirectoryInfo d = new DirectoryInfo(src);
            ArrayList FileList = new ArrayList();
            GetAll(d , FileList);

            Process p = new Process();
            p.StartInfo.FileName = "cmd.exe";
           
            p.StartInfo.UseShellExecute = false;
            p.StartInfo.RedirectStandardInput = true;
            p.Start();

            string exec_str = string.Empty;
            string space_str = " ";

            foreach (Object obj in FileList)
            {
                exec_str = string.Empty;
                exec_str += "copy";
                exec_str += space_str;
                exec_str += "/y";   //Always overwrite files with same names
                exec_str += space_str;
                exec_str += obj.ToString();
                exec_str += space_str;
                exec_str += target;
                p.StandardInput.WriteLine(exec_str);
            }
            

        }

        private static void CalcHashDup(string tarpath)
        {
            DirectoryInfo d = new DirectoryInfo(tarpath);
            ArrayList hashlist = new ArrayList();
            int hash_file_count = 0;
            FileInfo[] allFile = d.GetFiles();
            foreach(FileInfo fi in allFile)
            {
                if (fi.Name.EndsWith("-hash.txt"))
                {
                    StreamReader srReadFile = new StreamReader(fi.FullName);
                    while (!srReadFile.EndOfStream)
                    {
                        string strReadLine = srReadFile.ReadLine();
                        hashlist.Add(strReadLine);

                    }
                    srReadFile.Close();
                    hash_file_count++;

                }
            }
            LogService.LogService.WriteLog(hash_file_count.ToString() + " hash files are read.");
            LogService.LogService.WriteLog("Hash value container generated. " + hashlist.Count.ToString());
            int no_dup_cnt = 0;
            bool bDuphit = false;
            if (hashlist.Count != 0)
            {
                //2611 need to further refine the way of counting dups.
                for (int count1 = 0; count1 < hashlist.Count; count1++)
                {
                    for (int count2 = count1 + 1; count2 < hashlist.Count; count2++)
                    {
                        if (hashlist[count1].ToString() == hashlist[count2].ToString())
                        {
                            //filehashval.RemoveAt(count2);
                            //dup_cnt++;
                            bDuphit = true;
                           // LogService.LogService.WriteLog(" dup found is: " + dup_cnt.ToString());
                            LogService.LogService.WriteLog(count1.ToString() + " and " + count2.ToString() + " hash value is " + hashlist[count2].ToString());
                        }
                    }
                    if (!bDuphit)
                    {
                        no_dup_cnt++;
                    }
                    bDuphit = false; //Set to false for every complete count2 counting.

                }

            }
            LogService.LogService.WriteLog("Number of no dup chunks found in " + tarpath + " is:" + no_dup_cnt.ToString());


        }

        private static void LogAnalysis(string filepath)
        {
            //timestamp # unix timestamp # event type # EN/DE # text # TASK ID # chunk ID # data node
            // 0            1                   2           3   4       5           6           7
            // TaskID = FILENAME + REPLICA

            StreamReader srReadFile = new StreamReader(filepath);
            ArrayList timestamps_overall = new ArrayList();
            ArrayList overallList = new ArrayList();
            ArrayList task_status = new ArrayList();
            ArrayList task_id = new ArrayList();

            ArrayList timestamps_RIO = new ArrayList();
            ArrayList RIOList = new ArrayList();
            ArrayList RIO_status = new ArrayList();
            ArrayList RIO_taskid = new ArrayList();


            try
            {

                //Read all elements from the log file
                while (!srReadFile.EndOfStream)
                {


                    string strReadLine = srReadFile.ReadLine();

                    //Lazy filter: 2014 or 2015, AM or PM
                    if (strReadLine == string.Empty || strReadLine == null)
                    {
                        continue;
                    }
                    if (!strReadLine.Contains("2014") && !strReadLine.Contains("2015") && !strReadLine.Contains("AM") && !strReadLine.Contains("PM"))
                    {
                        continue; 
                    }

                    string[] arr = strReadLine.Split('#');

                    if (arr[2] != "RIO")
                    {
                        if (arr.Length < 6 || arr == null)
                        {
                            continue; //Ignore this log entry
                        }
                        double unixtime = Double.Parse(arr[1]);
                        timestamps_overall.Add(unixtime * 1000); // Convert to milliseconds unit
                        overallList.Add(arr[2]);
                        task_status.Add(arr[3]);

                        //To support multiple files, need to compare arr[5] as well
                        task_id.Add(arr[5]);

                    }
                    else // RIO
                    {
                        double unixtime = Double.Parse(arr[1]);
                        timestamps_RIO.Add(unixtime * 1000); // Convert to milliseconds unit
                        RIOList.Add(arr[2]);
                        RIO_status.Add(arr[3]);
                        RIO_taskid.Add(arr[5]);
                    }
                }

                srReadFile.Close();
            }
            catch (Exception ex)
            { 
            
            }
            //CNN DE - EN: Waiting time
            //There shouldn't be a long waiting time unless too many tasks are queuing.
            int count1 = 0;
            int count2 = 0;
            string str_taskid = string.Empty;

            double CNN_Waiting = 0;
            ArrayList arr_cnn_waiting = new ArrayList();
            ArrayList arr_cnn_service = new ArrayList();
            ArrayList arr_nnc_waiting = new ArrayList();
            ArrayList arr_nnc_service = new ArrayList();
            ArrayList arr_cdn_waiting = new ArrayList();
            ArrayList arr_cdn_dispatch = new ArrayList();


            for (int count = 0; count < overallList.Count; count++)
            {

                count1 = 0;
                count2 = 0;
                if (overallList[count].ToString() == "CNN" && task_status[count].ToString() == "EN")
                {
                    count1 = count;
                    str_taskid = task_id[count].ToString();
                }
                //Assume the DE event comes after the EN event..
                //int tempcount = 0;

                for (int tempcount = count + 1; tempcount < overallList.Count - 1; tempcount++)
                {
                    if (task_id[tempcount].ToString() != str_taskid)
                    {
                        continue;                    
                    }
                    if ((overallList[tempcount].ToString() == "CNN") && (task_status[tempcount].ToString() == "DE") && (task_id[tempcount].ToString() == str_taskid))
                    {
                        count2 = tempcount;
                    }
                
                }
                if (count1 == count2)
                { continue; }
                CNN_Waiting = (double)timestamps_overall[count2] - (double)timestamps_overall[count1]; // Unit: seconds
                
                
                arr_cnn_waiting.Add(CNN_Waiting);
                
                //CNN service
                int tempuse = count2;
                count1 = 0;
                count2 = 0;
                
                for (int count_cnn_service = 0; count_cnn_service < overallList.Count; count_cnn_service++)
                {

                    if ((overallList[count_cnn_service].ToString() == "NNC") && (task_status[count_cnn_service].ToString() == "EN") && (task_id[count_cnn_service].ToString() == str_taskid))
                    {
                        count2 = count_cnn_service;
                    }
                }
                
                double CNN_Service = 0; //clear
                if (tempuse == count2)
                { continue; }
                CNN_Service = (double)timestamps_overall[count2] - (double)timestamps_overall[tempuse]; // Unit: seconds
                arr_cnn_service.Add(CNN_Service);

                //NNC waiting
                 tempuse = count2;
                    count1 = 0;
                    count2 = 0;

                    for (int count_nnc_waiting = 0; count_nnc_waiting < overallList.Count; count_nnc_waiting++)
                    {

                        if ((overallList[count_nnc_waiting].ToString() == "NNC") && (task_status[count_nnc_waiting].ToString() == "DE") && (task_id[count_nnc_waiting].ToString() == str_taskid))
                        {
                            count2 = count_nnc_waiting;
                        }
                    }

                    double NNC_waiting = 0; //clear
                    if (tempuse == count2)
                    { continue; }
                    NNC_waiting = (double)timestamps_overall[count2] - (double)timestamps_overall[tempuse]; // Unit: seconds
                    arr_nnc_waiting.Add(NNC_waiting);
                //CDN
                    // CDN EN - NNC DE : Randomly assignment of datanodes
                    tempuse = count2;
                    count1 = 0;
                    count2 = 0;

                    for (int count_cdn = 0; count_cdn < overallList.Count; count_cdn++)
                    {

                        if ((overallList[count_cdn].ToString() == "CDN") && (task_status[count_cdn].ToString() == "EN") && (task_id[count_cdn].ToString() == str_taskid))
                        {
                            count2 = count_cdn;
                        }
                    }

                    double NNC_Servicing = 0; //clear
                    if (tempuse == count2)
                    { continue; }
                    NNC_Servicing = (double)timestamps_overall[count2] - (double)timestamps_overall[tempuse]; // Unit: seconds
                    arr_nnc_service.Add(NNC_Servicing);

                    // CDN DE - CDN EN
                    tempuse = count2;
                    count1 = 0;
                    count2 = 0;

                    for (int count_cdn_waiting = 0; count_cdn_waiting < overallList.Count; count_cdn_waiting++)
                    {

                        if ((overallList[count_cdn_waiting].ToString() == "CDN") && (task_status[count_cdn_waiting].ToString() == "DE") && (task_id[count_cdn_waiting].ToString() == str_taskid))
                        {
                            count2 = count_cdn_waiting;
                        }
                    }

                    double CDN_waiting = 0; //clear
                    if (tempuse == count2)
                    { continue; }
                    CDN_waiting = (double)timestamps_overall[count2] - (double)timestamps_overall[tempuse]; // Unit: seconds
                    arr_cdn_waiting.Add(CDN_waiting);

                    //Capture the last RIO en event
                    // it minus the CDN DE is the servicing time of dispatching write requests
                    tempuse = count2;
                    count1 = 0;
                    count2 = 0;

                    for (int rio_count = 0; rio_count < RIOList.Count; rio_count++)
                    {
                        if (RIO_status[rio_count].ToString() == "EN")
                        {
                            count1 = rio_count; //Refresh until the latest one.
                        }
                    }
                    double CDN_dispatching = 0;
                    if (tempuse == count1)
                    { continue; }
                    CDN_dispatching = (double)timestamps_RIO[count1] - (double)timestamps_overall[tempuse]; // Unit: seconds
                    arr_cdn_dispatch.Add(CDN_dispatching);
                    
            }

            
            

            //LogService.LogService.WriteLog(timestamps_overall[count2].ToString() + " minus " + timestamps_overall[count1]);
            //NNC EN - CNN DE: Service time (network delay)
            

            //LogService.LogService.WriteLog(timestamps_overall[count2].ToString() + " minus " + timestamps_overall[tempuse]);


            // NNC DE - NNC EN : Waiting time
           

           

           
            

            // Capture the waiting time of all RIO events

            LogService.LogService.WriteLog("RIO counted: " + RIOList.Count.ToString() + " " + RIO_taskid.Count.ToString() + " " + RIO_status.Count.ToString());
            ArrayList rio_waiting = new ArrayList();
            for (int rio_count1 = 0; rio_count1 < RIOList.Count; rio_count1++)
            {
                for (int rio_count2 = rio_count1 + 1; rio_count2 < RIOList.Count; rio_count2++)
                {
                    if ((RIO_taskid[rio_count1].ToString() == RIO_taskid[rio_count2].ToString()) && (RIO_status[rio_count1].ToString() == "EN") && (RIO_status[rio_count2].ToString() == "DE"))
                    {
                        double RIO_waiting = (double)timestamps_RIO[rio_count2] - (double)timestamps_RIO[rio_count1];
                        rio_waiting.Add(RIO_waiting);
                        //LogService.LogService.WriteLog("RIO waiting counted");
                    }
                }
            }

            double mean_rio_waiting = 0;
            foreach (double dl in rio_waiting)
            {
                mean_rio_waiting += dl;
            }
            mean_rio_waiting = mean_rio_waiting / rio_waiting.Count;

            // Capture the SERVICE time of all RIO events
            

            ArrayList rio_servicing = new ArrayList();
            ArrayList rio_dn_waiting = new ArrayList();
            ArrayList rio_ssd_writing = new ArrayList();
            ArrayList rio_network_delay = new ArrayList();
            for (int rio_count1 = 0; rio_count1 < RIOList.Count; rio_count1++)
            {
                for (int rio_count2 = rio_count1 + 1; rio_count2 < RIOList.Count; rio_count2++)
                {
                    if ((RIO_taskid[rio_count1].ToString() == RIO_taskid[rio_count2].ToString()) && (RIO_status[rio_count1].ToString() == "DE") && (RIO_status[rio_count2].ToString() == "FN"))
                    {
                        double RIO_service = (double)timestamps_RIO[rio_count2] - (double)timestamps_RIO[rio_count1];
                        rio_servicing.Add(RIO_service);
                        //LogService.LogService.WriteLog("RIO servicing counted");
                    }
                    if ((RIO_taskid[rio_count1].ToString() == RIO_taskid[rio_count2].ToString()) && (RIO_status[rio_count1].ToString() == "DNEQ") && (RIO_status[rio_count2].ToString() == "DNDQ"))
                    {
                        double RIO_service = (double)timestamps_RIO[rio_count2] - (double)timestamps_RIO[rio_count1];
                        rio_dn_waiting.Add(RIO_service);
                        //LogService.LogService.WriteLog("RIO servicing counted");
                    }
                    if ((RIO_taskid[rio_count1].ToString() == RIO_taskid[rio_count2].ToString()) && (RIO_status[rio_count1].ToString() == "DNDQ") && (RIO_status[rio_count2].ToString() == "SSDW"))
                    {
                        double RIO_service = (double)timestamps_RIO[rio_count2] - (double)timestamps_RIO[rio_count1];
                        rio_ssd_writing.Add(RIO_service);
                        //LogService.LogService.WriteLog("RIO servicing counted");
                    }
                }
            }
            const double min_factor = 500;
            double mean_rio_service = 0;
            double min_rio_service = min_factor;
            double max_rio_service = 0;

            foreach (double dl in rio_servicing)
            {
                mean_rio_service += dl;
                if (dl < min_rio_service)
                {
                    min_rio_service = dl;
                }
                if (dl > max_rio_service)
                {
                    max_rio_service = dl;
                }
            }
            mean_rio_service = mean_rio_service / rio_servicing.Count;  // Mean of rio service time
             // Data node waiting time
            
            double mean_rio_dn_waiting = 0;
            double min_rio_dn_waiting = min_factor;
            double max_rio_dn_waiting = 0;

            foreach (double dl in rio_dn_waiting)
            {
                mean_rio_dn_waiting += dl;
                if (dl < min_rio_dn_waiting)
                {
                    min_rio_dn_waiting = dl;
                }
                if (dl > max_rio_dn_waiting)
                {
                    max_rio_dn_waiting = dl;
                }
            }
            mean_rio_dn_waiting = mean_rio_dn_waiting / rio_dn_waiting.Count;  // Mean of rio service time

            //SSD writing at each data node
            double mean_rio_ssd_writing = 0;
            double min_rio_ssd_writing = min_factor;
            double max_rio_ssd_writing = 0;

            foreach (double dl in rio_ssd_writing)
            {
                mean_rio_ssd_writing += dl;
                if (dl < min_rio_ssd_writing)
                {
                    min_rio_ssd_writing = dl;
                }
                if (dl > max_rio_ssd_writing)
                {
                    max_rio_ssd_writing = dl;
                }
            }
            mean_rio_ssd_writing = mean_rio_ssd_writing / rio_ssd_writing.Count;  // Mean of rio service time


            //Add max and min service time here for RIO:
            double mean_cnn_waiting = 0;
            double min_cnn_waiting = min_factor;
            double max_cnn_waiting = 0;

            foreach (double dl in arr_cnn_waiting)
            {
                mean_cnn_waiting += dl;
                if (dl < min_cnn_waiting)
                {
                    min_cnn_waiting = dl;
                }
                if (dl > max_cnn_waiting)
                {
                    max_cnn_waiting = dl;
                }
            }
            mean_cnn_waiting = mean_cnn_waiting / arr_cnn_waiting.Count;  // Mean of cnn waiting

            double mean_cnn_service = 0;
            double max_cnn_service = 0;
            double min_cnn_service = min_factor;

            foreach (double dl in arr_cnn_service)
            {
                mean_cnn_service += dl;
                if (dl < min_cnn_service)
                {
                    min_cnn_service = dl;
                }
                if (dl > max_cnn_service)
                {
                    max_cnn_service = dl;
                }
            }
            mean_cnn_service = mean_cnn_service / arr_cnn_service.Count;  // Mean of cnn service

            double mean_nnc_waiting = 0;
            double max_nnc_waiting = 0;
            double min_nnc_waiting = min_factor;

            foreach (double dl in arr_nnc_waiting)
            {
                mean_nnc_waiting += dl;
                if (dl < min_nnc_waiting)
                {
                    min_nnc_waiting = dl;
                }
                if (dl > max_nnc_waiting)
                {
                    max_nnc_waiting = dl;
                }
            }
            mean_nnc_waiting = mean_nnc_waiting / arr_nnc_waiting.Count;  // Mean of nnc waiting

            double mean_nnc_service = 0;
            double max_nnc_service = 0;
            double min_nnc_service = min_factor;


            foreach (double dl in arr_nnc_service)
            {
                mean_nnc_service += dl;
                if (dl < min_nnc_service)
                {
                    min_nnc_service = dl;
                }
                if (dl > max_nnc_service)
                {
                    max_nnc_service = dl;
                }
            }
            mean_nnc_service = mean_nnc_service / arr_nnc_service.Count;  // Mean of nnc service

            double mean_cdn_waiting = 0;
            double max_cdn_waiting = 0;
            double min_cdn_waiting = min_factor;


            foreach (double dl in arr_cdn_waiting)
            {
                mean_cdn_waiting += dl;
                if (dl < min_cdn_waiting)
                {
                    min_cdn_waiting = dl;
                }
                if (dl > max_cdn_waiting)
                {
                    max_cdn_waiting = dl;
                }
            }
            mean_cdn_waiting = mean_cdn_waiting / arr_cdn_waiting.Count;  // Mean of nnc service

            double mean_cdn_dispatch = 0;
            double max_cdn_dispatch = 0;
            double min_cdn_dispatch = min_factor;

            foreach (double dl in arr_cdn_dispatch)
            {
                mean_cdn_dispatch += dl;
                if (dl < min_cdn_dispatch)
                {
                    min_cdn_dispatch = dl;
                }
                if (dl > max_cdn_dispatch)
                {
                    max_cdn_dispatch = dl;
                }
            }
            mean_cdn_dispatch = mean_cdn_dispatch / arr_cdn_dispatch.Count;  // Mean of nnc service

            //Print result
            string log_analysis = string.Empty;
            log_analysis += "Here are the statistics of previous run:" + '\n';
            log_analysis += "CNN waiting mean: " + '\t' + '\t' + "Max: " + '\t' + '\t' + "Min: " + '\n';
            log_analysis += mean_cnn_waiting.ToString() + '\t' + '\t' + max_cnn_waiting.ToString() + '\t' + '\t' + min_cnn_waiting.ToString() + '\n';
            log_analysis += "CNN service mean: " + '\t' + '\t' + "Max: " + '\t' + '\t' + "Min: " + '\n';
            log_analysis += mean_cnn_service.ToString() + '\t' + '\t' + max_cnn_service.ToString() + '\t' + '\t' + min_cnn_service.ToString() + '\n';
            log_analysis += "NNC waiting mean: " + '\t' + '\t' + "Max: " + '\t' + '\t' + "Min: " + '\n';
            log_analysis += mean_nnc_waiting.ToString() + '\t' + '\t' + max_nnc_waiting.ToString() + '\t' + '\t' + min_nnc_waiting.ToString() + '\n';
            log_analysis += "NNC servicing mean: " + '\t'  + "Max: " + '\t'  + "Min: " + '\n';
            log_analysis += mean_nnc_service.ToString() + '\t' + '\t' + max_nnc_service.ToString() + '\t' + '\t' + min_nnc_service.ToString() + '\n';
            log_analysis += "CDN waiting mean: " + '\t' + "Max: " + '\t' + "Min: " + '\n';
            log_analysis += mean_cdn_waiting.ToString() + '\t' + '\t' + max_cdn_waiting.ToString() + '\t' + '\t' + min_cdn_waiting.ToString() + '\n';
            log_analysis += "CDN dispatching mean: " + '\t' + "Max: " + '\t' + "Min: " + '\n';
            log_analysis += mean_cdn_dispatch.ToString() + '\t' + '\t' + max_cdn_dispatch.ToString() + '\t' + '\t' + min_cdn_dispatch.ToString() + '\n';

            log_analysis += "Mean waiting time RIO:" + mean_rio_waiting.ToString() + '\n';
            log_analysis += "Mean servicing time RIO:" + mean_rio_service.ToString() + '\n';
            log_analysis += "Max servicing time RIO:" + max_rio_service.ToString() + '\n';
            log_analysis += "Min servicing time RIO:" + min_rio_service.ToString() + '\n';

            log_analysis += "Mean RIO-DN WAITING:" + mean_rio_dn_waiting.ToString() + '\n';
            log_analysis += "Max RIO-DN WAITING:" + max_rio_dn_waiting.ToString() + '\n';
            log_analysis += "Min RIO-DN WAITING:" + min_rio_dn_waiting.ToString() + '\n';

            log_analysis += "Mean RIO-SSD writing:" + mean_rio_ssd_writing.ToString() + '\n';
            log_analysis += "Max RIO-SSD writing:" + max_rio_ssd_writing.ToString() + '\n';
            log_analysis += "Min RIO-SSD writing:" + min_rio_ssd_writing.ToString() + '\n';

            LogService.LogService.WriteLog(log_analysis);
            
        }

        private static void DataNodeDistr(string path)
        {
            Dictionary<int, int> dc_cnt = new Dictionary<int, int>();
            const int node_num = 19;
            ArrayList node_cnt = new ArrayList();
            for (int i = 0; i < node_num; i++)
            {
                node_cnt.Add(0);
            }

            XmlDocument xmldoc = new XmlDocument();
            xmldoc.Load(path);

            XmlNode root = xmldoc.SelectSingleNode("meta");
            XmlNodeList filenodes = root.SelectNodes("filemeta");
            if (filenodes != null)
            {
                foreach (XmlNode filenode in filenodes)
                {
                    XmlNodeList chunklist = filenode.ChildNodes;
                    if (chunklist != null)
                    {
                        foreach (XmlNode chunkNode in chunklist)
                        {
                            int node_id = Int32.Parse(chunkNode.Attributes["DatanodeID"].Value);
                            int temp_cnt = (int)node_cnt[node_id];
                            temp_cnt++;
                            node_cnt[node_id] = (Object)temp_cnt;
                            node_id = Int32.Parse(chunkNode.Attributes["SecDatanodeID"].Value);
                            temp_cnt = 0;
                            temp_cnt = (int)node_cnt[node_id];
                            temp_cnt++;
                            node_cnt[node_id] = (Object)temp_cnt;
                            node_id = Int32.Parse(chunkNode.Attributes["TerDatanodeID"].Value);
                            temp_cnt = 0;
                            temp_cnt = (int)node_cnt[node_id];
                            temp_cnt++;
                            node_cnt[node_id] = (Object)temp_cnt;
                        }
                     }
                }
            }
            double mean = 0;
            int max = 0;
            int min = 5000;
            //double sd = 0;

            int total = 0;
            foreach (object obj in node_cnt)
            {
                int temp = (int)obj;
                if (temp > max)
                {
                    max = temp;
                }
                if (temp < min)
                {
                    min = temp;
                }
                total += temp;
            }
            mean = total / node_cnt.Count;
            double deviation = 0;
            foreach (object obj in node_cnt)
            {
                double value = Double.Parse(obj.ToString());
                double diff = value - mean;
                double diff_sq = diff * diff;
                deviation += diff_sq;
            }
            deviation /= node_cnt.Count;
            deviation = System.Math.Sqrt(deviation);  

            string xml_node_log = string.Empty;
            xml_node_log += "Node distribution:" + '\t' + "Node ID: " + '\t' + "Occurence" + '\n';
            for (int nodeid = 0; nodeid < node_num; nodeid++ )
            {
                xml_node_log += "        " + '\t' + nodeid.ToString() + '\t' + node_cnt[nodeid].ToString() + '\n';
            }
            xml_node_log += "Max: " + max.ToString() + '\n';
            xml_node_log += "Min: " + min.ToString() + '\n';
            xml_node_log += "Mean: " + mean.ToString() + '\n';
            xml_node_log += "SD: " + deviation.ToString() + '\n';
            

            LogService.LogService.WriteLog(xml_node_log);

        }
    }
   
}
