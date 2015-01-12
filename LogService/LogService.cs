using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.IO;

namespace LogService
{
    public class LogService
    {
        private static string logfilepath = string.Empty;
        private static string loglevel = string.Empty;
        private static bool bDebug = false;
        public LogService()
        {

            logfilepath = System.Environment.CurrentDirectory + ConfigurationManager.AppSettings["logfilepath"];
            loglevel = ConfigurationManager.AppSettings["logfilepath"];

            if (loglevel == "debug")
            {
                bDebug = true;
            }

        }

        public static void ClearLog()
        {

            FileStream fs = null;
            try
            {
                fs = new FileStream(logfilepath, FileMode.Truncate, FileAccess.ReadWrite);

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

        public static void WriteDebug(string str)
        {
            if (bDebug)
            {
                WriteLog(str);

            }
            //Otherwise do nothing.
        }

        public static void WriteLog(string str)
        {


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

            tempstr += str;  // Append passed in strings.
            FileStream fs = null;
            try
            {

                fs = new FileStream(logfilepath, FileMode.Append, FileAccess.Write, FileShare.Write);
                if (fs != null)
                {
                    StreamWriter sw = new StreamWriter(fs);
                    //sw.Flush();
                    sw.WriteLine(tempstr);
                    sw.Close();
                    fs.Close();
                }
                
            }
            catch (Exception ex)
            {
                Console.WriteLine("Log file IO failed：" + ex.Message);
            }
            finally
            {
                //fs.Close();
            }
        }

        private static long UnixTimeNow()
        {
            var timeSpan = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
            return (long)timeSpan.TotalSeconds;
        }

    }
}
