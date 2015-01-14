using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Configuration;
using System.IO;

namespace LogService
{
    public class LogService
    {

        private static readonly Thread WriteThread;
        private static readonly Queue<string> MsgQueue;
        private static readonly object FileLock;
        private static string fileName = string.Empty;
        //private static readonly string FilePath;

        static LogService()
        {
            FileLock = new object();
            fileName = System.Environment.CurrentDirectory + ConfigurationManager.AppSettings["logfilepath"];
            WriteThread = new Thread(WriteMsg);
            MsgQueue = new Queue<string>();
            WriteThread.Start();
        }

        /*
        public static void LogInfo(string msg)
        {
            Monitor.Enter(MsgQueue);
            MsgQueue.Enqueue(string.Format("{0} {1} {2}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:sss"), "Info", msg));
            Monitor.Exit(MsgQueue);
        }
        public static void LogError(string msg)
        {
            Monitor.Enter(MsgQueue);
            MsgQueue.Enqueue(string.Format("{0} {1} {2}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:sss"), "Error", msg));
            Monitor.Exit(MsgQueue);
        }
         */
        public static void WriteLog(string msg)
        {
            Monitor.Enter(MsgQueue);
            //Get timestamps
            string tempstr = string.Empty;
            tempstr += DateTime.Now.ToString();
            tempstr += " " + DateTime.Now.Millisecond.ToString();
            tempstr += "#";
            // UNIX time + milliseconds
            long unix_time = UnixTimeNow();
            double unix_final = unix_time + (double)DateTime.UtcNow.Millisecond / 1000;
            tempstr += unix_final.ToString();

            tempstr += "#";

            tempstr += msg;  // Append passed in strings.
            MsgQueue.Enqueue(string.Format("{0}", tempstr));
            Monitor.Exit(MsgQueue);
        }

        private static long UnixTimeNow()
        {
            var timeSpan = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
            return (long)timeSpan.TotalSeconds;
        }
        private static void WriteMsg()
        {
            while (true)
            {
                if (MsgQueue.Count > 0)
                {
                    Monitor.Enter(MsgQueue);
                    string msg = MsgQueue.Dequeue();
                    Monitor.Exit(MsgQueue);

                    Monitor.Enter(FileLock);
                    /*
                    if (!Directory.Exists(FilePath))
                    {
                        Directory.CreateDirectory(FilePath);
                    }
                     */
                    //string fileName = FilePath + DateTime.Now.ToString("yyyy-MM-dd") + ".log";
                    
                    var logStreamWriter = new StreamWriter(fileName, true);

                    logStreamWriter.WriteLine(msg);
                    logStreamWriter.Close();
                    Monitor.Exit(FileLock);

                    /*
                     if (GetFileSize(fileName) > 1024 * 5)
                    {
                        CopyToBak(fileName);
                    }
                     */
                }

            }
        }
        public static void ClearLog()
        {

            FileStream fs = null;
            try
            {
                fs = new FileStream(fileName, FileMode.Truncate, FileAccess.ReadWrite);

            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to clear log file：" + ex.Message);
            }
            finally
            {
                fs.Close();
            }

        }
        private static long GetFileSize(string fileName)
        {
            long strRe = 0;
            if (File.Exists(fileName))
            {
                Monitor.Enter(FileLock);
                var myFs = new FileStream(fileName, FileMode.Open);
                strRe = myFs.Length / 1024;
                myFs.Close();
                myFs.Dispose();
                Monitor.Exit(FileLock);
            }
            return strRe;
        }
        private static void CopyToBak(string sFileName)
        {
            int fileCount = 0;
            string sBakName = "";
            Monitor.Enter(FileLock);
            do
            {
                fileCount++;
                sBakName = sFileName + "." + fileCount + ".BAK";
            }
            while (File.Exists(sBakName));

            File.Copy(sFileName, sBakName);
            File.Delete(sFileName);
            Monitor.Exit(FileLock);
        }
    }
}
