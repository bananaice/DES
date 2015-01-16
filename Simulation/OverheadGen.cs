using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Simulation
{
    class OverheadGen
    {
        //source: http://blog.163.com/hlz_2599/blog/static/142378474201341341339314/
        //source: http://wiki.networksecuritytoolkit.org/nstwiki/index.php/LAN_Ethernet_Maximum_Rates,_Generation,_Capturing_%26_Monitoring
        //For UDP transmission over 100Mbps ethernet
        //Average speed  = 3.4MB/s
        //1MB - 300ms
        //256KB - 80ms
        //64kB - 20ms

        //Otherwise, if the request has been req only with minimal data e.g. <10KB
        //Say the network delay is ~5ms

        //Theoretical rate for TCP/IP over 1Gb link
        //Average speed = 123MB
        //We use 50MB / s
        //4MB = 80ms
        //1MB = 20ms
        //256KB = 6ms
        //<64KB = 2ms

        //Otherwise, if the request has been req only with minimal data e.g. <10KB
        //Say the network delay is ~5ms

        public static int ReturnNetworkDelay(int sizeinKB, bool bReqOnly)
        {
            int retVal = 0; //in milliseconds
            int offset = 0;
            if (bReqOnly) //req only transmission
            {
                retVal = 2;
                offset = 1;
            }
            else //data blocks
            {
                if (sizeinKB <= 64)
                {
                    retVal = 2;
                    offset = 1;
                }
                else if (sizeinKB > 64 && sizeinKB <= 256)
                {
                    retVal = 6;
                    offset = 2;
                }
                else if (sizeinKB > 256 && sizeinKB <= 1000)
                {
                    retVal = 20;
                    offset = 4;
                }
                else 
                {
                    retVal = (sizeinKB / 1000) * 20;
                    offset = retVal / 5;
                }
            }
            Random rd = new Random();
            retVal = retVal + rd.Next(-offset, offset);

            
            return retVal; 
        }
    }
}
