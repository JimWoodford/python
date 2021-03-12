#Standard Imports
from datetime import datetime

import win32api
import win32con
import win32security

import logging
import os
import sys
import pyodbc
import shutil

#Third Party Imports
import pandas as pd
import numpy as np
import uuid
import xlrd

#Local Imports
from local_settings import sourcePath,pyodbcDriver,pyodbcServer,pyodbcDatabase,pyodbcUser

#Special Imports
sys.path.append(r'C:\Dev\Utilities\SMTP')

from SMTP_Notification import Email

def main()
    processPermissionPassPass()
    sendEmail()

if __name__ == "__main__":
    main()