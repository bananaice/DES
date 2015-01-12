using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Security;
using System.Collections;
using System.Configuration;
using LogService;

namespace PreProcessor
{
    class FileChunker
    {
        private string src = null;
        private string tar = null;
        private string src_dir_path = string.Empty;
        private string dest_dir_path = string.Empty;
        private const string strDelimiter = "\\";
        private int filesize;
        public FileChunker(string filename)
        {
            src = filename;
            tar = string.Empty;
        }

        public FileChunker(string src_dir, string dest_dir)
        {
            src_dir_path = src_dir;
            dest_dir_path = dest_dir;
        }

        public bool BuildBatchChunk()
        {

            //Traverse all files flatly in the src directory

            if (src_dir_path == string.Empty)
            {
                return false;
            }


            if (dest_dir_path == string.Empty)
            {
                return false;
            }

            ArrayList FileNameList = new ArrayList();
            ArrayList FilePathList = new ArrayList();
            DirectoryInfo d = new DirectoryInfo(src_dir_path);

            FileInfo[] allFile = d.GetFiles();
            foreach (FileInfo fi in allFile)
            {
                //Only add bigger files with size >= 16KB

                FileNameList.Add(fi.Name);
                FilePathList.Add(fi.FullName);
                                
            }

            filesize = Int32.Parse(ConfigurationManager.AppSettings["filesize"]);
            string ifhash = ConfigurationManager.AppSettings["hash"];
            bool bHash = false;
            if (ifhash == "yes")
            {
                bHash = true;
            }
            else
            {
                bHash = false;
            }
            //Used for calculation / statistics
            //ArrayList filehashval = new ArrayList();

            //Recursively chunking

            for (int mycount = 0; mycount < FileNameList.Count; mycount++)
            {

                FileStream SplitFileStream = null;
                BinaryReader SplitFileReader = null;

                try
                {

                    SplitFileStream = new FileStream(FilePathList[mycount].ToString(), FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);

                    SplitFileReader = new BinaryReader(SplitFileStream);

                    byte[] TempBytes;
                    //Buffer the chunking

                    //string[] str_ext = src.Split('.'); //Assume only one dot in between extensions

                    uint iFileCount = (uint)(SplitFileStream.Length / filesize);

                    

                    if (SplitFileStream.Length % filesize != 0) //Minimal number of chunk is one.
                    {
                        iFileCount++;
                    }

                    string logstr = "Chunk size is: ";
                    logstr += (filesize / 1024).ToString();
                    logstr += "KB. ";
                    logstr += "Chunk number will be: ";
                    logstr += iFileCount.ToString();
                    LogService.LogService.WriteLog(logstr);

                    //Create the output file storing the hash values in chunk orders
                    string file_hashout = dest_dir_path + FileNameList[mycount].ToString() + "-hash.txt";
                    FileStream hashout = new FileStream(file_hashout, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                    hashout.Flush();
                    hashout.Close();



                    for (int i = 0; i < iFileCount; i++)
                    {
                        //string sTempFileName = src + strDelimiter + str_ext[1] + i.ToString(); //Chunk names
                        string sTempFileName = dest_dir_path + FileNameList[mycount].ToString() + i.ToString(); //Chunk names
                        FileStream TempStream = new FileStream(sTempFileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                        //Instantiate the file stream according to the chunk name and its open status


                        //Instead of using temp write to write chunks into file, calculate the hash value and move on...

                        BinaryWriter TempWriter = new BinaryWriter(TempStream);
                        TempBytes = SplitFileReader.ReadBytes(filesize);
                        //Read designated size from the original file
                        TempWriter.Write(TempBytes);
                        //Write data into chunks
                        TempWriter.Close();
                        //Closure of the writer



                        //Cal SHA1 hash value
                        if (bHash)
                        {
                            String hashSHA1 = String.Empty;

                            System.Security.Cryptography.SHA1 calculator = System.Security.Cryptography.SHA1.Create();
                            Byte[] buffer = calculator.ComputeHash(TempBytes);
                            calculator.Clear();
                            //Convert into hexical format
                            StringBuilder stringBuilder = new StringBuilder();
                            for (int icnt = 0; icnt < buffer.Length; icnt++)
                            {
                                stringBuilder.Append(buffer[icnt].ToString("x2"));
                            }

                            hashSHA1 = stringBuilder.ToString();

                            //Write to file
                            hashout = new FileStream(file_hashout, FileMode.Append, FileAccess.Write, FileShare.Write);
                            if (hashout != null)
                            {
                                StreamWriter sw = new StreamWriter(hashout);
                                //sw.Flush();
                                sw.WriteLine(hashSHA1);
                                sw.Close();

                                //filehashval.Add(hashSHA1);

                            }
                            hashout.Close();
                        }



                        TempStream.Close();
                        //Closure of the stream

                    }
                    SplitFileReader.Close();
                    //Closure
                    SplitFileStream.Close();

                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    return false;
                }
                finally
                {
                    SplitFileReader.Close();
                    //Closure
                    SplitFileStream.Close();

                }
            }
            
            //no longer use the code.
                
            /*
                //Run a dedup search across files
                int dup_cnt = 0;
                if (filehashval != null)
                {

                    for (int count1 = 0; count1 < filehashval.Count; count1++)
                    {
                        for (int count2 = count1 + 1; count2 < filehashval.Count; count2++)
                        {
                            if (filehashval[count1] == filehashval[count2])
                            {
                                //filehashval.RemoveAt(count2);
                                dup_cnt++;

                            }
                        }
                    }

                }
                LogService.LogService.WriteLog("Duplicates found in " + src.ToString() + " is:" + dup_cnt.ToString());
            */

                return true;
            
            
            
        }

        public bool BuildChunk()
        {
            FileStream SplitFileStream = new FileStream(src, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);

            BinaryReader SplitFileReader = new BinaryReader(SplitFileStream);

            try
            {

                filesize = Int32.Parse(ConfigurationManager.AppSettings["filesize"]);
                string ifhash = ConfigurationManager.AppSettings["hash"];
                bool bHash = false;
                if (ifhash == "yes")
                {
                    bHash = true;
                }
                else
                {
                    bHash = false;
                }

                



                byte[] TempBytes;
                //Buffer the chunking

                string[] str_ext = src.Split('.'); //Assume only one dot in between extensions

                uint iFileCount = (uint)(SplitFileStream.Length / filesize);

                string logstr = "Chunk size is: ";
                logstr += (filesize / 1024).ToString();
                logstr += "KB. ";
                logstr += "Chunk number will be: ";
                logstr += iFileCount.ToString();
                LogService.LogService.WriteLog(logstr);

                if (SplitFileStream.Length % filesize != 0)
                {
                    iFileCount++;
                }

                //Create the output file storing the hash values in chunk orders
                string file_hashout = src + "hash.txt";
                FileStream hashout = new FileStream(file_hashout, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                hashout.Close();

                ArrayList filehashval = new ArrayList();

                for (int i = 0; i < iFileCount; i++)
                {
                    //string sTempFileName = src + strDelimiter + str_ext[1] + i.ToString(); //Chunk names
                    string sTempFileName = src + i.ToString(); //Chunk names
                    FileStream TempStream = new FileStream(sTempFileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                    //Instantiate the file stream according to the chunk name and its open status


                    //Instead of using temp write to write chunks into file, calculate the hash value and move on...

                    BinaryWriter TempWriter = new BinaryWriter(TempStream);
                    TempBytes = SplitFileReader.ReadBytes(filesize);
                    //Read designated size from the original file
                    TempWriter.Write(TempBytes);
                    //Write data into chunks
                    TempWriter.Close();
                    //Closure of the writer

                   

                    //Cal SHA1 hash value
                    if (bHash)
                    {
                        String hashSHA1 = String.Empty;

                        System.Security.Cryptography.SHA1 calculator = System.Security.Cryptography.SHA1.Create();
                        Byte[] buffer = calculator.ComputeHash(TempBytes);
                        calculator.Clear();
                        //Convert into hexical format
                        StringBuilder stringBuilder = new StringBuilder();
                        for (int icnt = 0; icnt < buffer.Length; icnt++)
                        {
                            stringBuilder.Append(buffer[icnt].ToString("x2"));
                        }

                        hashSHA1 = stringBuilder.ToString();

                        //Write to file
                        hashout = new FileStream(file_hashout, FileMode.Append, FileAccess.Write, FileShare.Write);
                        if (hashout != null)
                        {
                            StreamWriter sw = new StreamWriter(hashout);
                            //sw.Flush();
                            sw.WriteLine(hashSHA1);
                            sw.Close();

                            filehashval.Add(hashSHA1);
                            
                        }
                        hashout.Close();
                    }

                    

                    TempStream.Close();
                    //Closure of the stream

                }
                SplitFileReader.Close();
                //Closure
                SplitFileStream.Close();

                //Delete the original file

                //It does not have to delete the original file
                /*
                if(File.Exists(src))
                {
                    File.Delete(src);
                }
                 */

                //Run a dedup search within the file 
                int dup_cnt = 0;
                if (filehashval != null)
                {
                    
                    for (int count1 = 0; count1 < filehashval.Count; count1++)
                    {
                        for (int count2 = count1 + 1; count2 < filehashval.Count; count2++)
                        {
                            if (filehashval[count1] == filehashval[count2])
                            {
                                //filehashval.RemoveAt(count2);
                                dup_cnt++;
                                
                            }
                        }
                    }
                
                }
                LogService.LogService.WriteLog("Duplicates found in " + src.ToString() + " is:" + dup_cnt.ToString());
                

                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                return false;
            }
            finally
            {
                SplitFileReader.Close();
                //Closure
                SplitFileStream.Close();               

            }
        
        }
        public uint BuildChunkAndHash()
        {
            FileStream SplitFileStream = new FileStream(src, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
              
             BinaryReader SplitFileReader = new BinaryReader(SplitFileStream);

             FileStream TempStream = null;
                
            try
            {
                
                byte[] TempBytes;
                //Buffer the chunking

                string[] str_ext = src.Split('.'); //Assume only one dot in between extensions

                uint iFileCount = (uint)(SplitFileStream.Length / filesize);

                if (SplitFileStream.Length % filesize != 0)
                {
                    iFileCount++;
                }
                for ( int i = 1 ; i <= iFileCount ; i++ )
　　            {
                  string sTempFileName = tar + strDelimiter + str_ext[1] + i.ToString(); //Chunk names
                  TempStream = new FileStream(sTempFileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
　　　            //Instantiate the file stream according to the chunk name and its open status


                    //Instead of using temp write to write chunks into file, calculate the hash value and move on...

　　　            BinaryWriter TempWriter = new BinaryWriter ( TempStream ) ;
　　　            TempBytes = SplitFileReader.ReadBytes ( filesize ) ;
　　　            //Read designated size from the original file
　　　            TempWriter.Write ( TempBytes ) ;
　　　            //Write data into chunks
　　　            TempWriter.Close ( ) ;
　　　            //Closure of the writer


                    //Cal SHA1 hash value
                 String hashSHA1 = String.Empty;

                 System.Security.Cryptography.SHA1 calculator = System.Security.Cryptography.SHA1.Create();
                 Byte[] buffer = calculator.ComputeHash(TempBytes);
                 calculator.Clear();
                 //Convert into hexical format
                 StringBuilder stringBuilder = new StringBuilder();
                 for (int icnt = 0; icnt < buffer.Length; icnt++)
                 {
                     stringBuilder.Append(buffer[icnt].ToString("x2"));
                 }

                 hashSHA1 = stringBuilder.ToString();


　　　            TempStream.Close ( ) ;
　　　            //Closure of the stream
　　　            
　　            }
　　            SplitFileReader.Close( );
　　            //Closure
　　            SplitFileStream.Close( );

              return iFileCount;
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.ToString());
                return 0; //Means error
            }
            finally
            {
                SplitFileReader.Close ( );
　　            //Closure
                SplitFileStream.Close(); 
                if(TempStream != null)
                {
                    TempStream.Close( ) ;
                }
                
            }
        }

    }
}
