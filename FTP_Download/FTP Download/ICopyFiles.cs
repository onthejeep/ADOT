using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FTP_Download
{
    interface ICopyFiles
    {
        void CopyFile(string ipAddress, string port, string path, string fileName);
        void RenameFile();
    }
}
