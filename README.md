# esxi_standalone_patcher
This Python 3 script helps update standalone ESXi hosts that cannot be connected to vCSA. I wrote this script specifically due to the need for automatic updates of remote hosts without internet, and vCSA access.

To start using this script, you need to install the latest version of Python 3 and the libraries from requirements.txt, then run the script "esxi_patcher.py". 
A simpler way is to use PyCharm, which quickly highlights and shows what needs to be installed. https://www.jetbrains.com/pycharm/download/?section=windows

All you need to do is adjust the configuration file(config.ini) with the values you require, and then the patches will be applied to the ESXi hosts.
