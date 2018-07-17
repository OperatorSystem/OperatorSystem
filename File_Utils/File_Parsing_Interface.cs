using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
namespace File_Utils
{
    interface ICheckDuplicate
    {
        bool IsDuplicate(string FileName);

        bool CheckFileFormat(string FileName);
        bool IsValidate(String FileName);
        bool FileParse(String FilePath, string FileName);
        bool FileGenerate(String FilePath);
    }
}