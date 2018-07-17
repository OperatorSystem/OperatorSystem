using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace File_Utils
{
    class TIC_Format : ICheckDuplicate
    {

        public bool CheckFileFormat(string FileName)
        {
            return true;
        }
            public bool IsDuplicate(string FileName)
        {
            bool result = false;           
            return result;
        }

        public bool IsValidate(string FileName)
        {
            return true;
        }

        public bool FileParse(string File_Path, string FileName)
        {
            return true;
        }

        public bool FileGenerate(string FileName)
        {
            return true;
        }
    }
}
