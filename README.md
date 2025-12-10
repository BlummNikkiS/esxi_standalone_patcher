# esxi_standalone_patcher
This Python 3 script helps update standalone ESXi hosts that cannot be connected to vCSA. I wrote this script specifically due to the need for automatic updates of remote hosts without internet, and vCSA access.

To start using this script, you need to install the latest version of Python 3 and the libraries from requirements.txt, then run the script "esxi_patcher.py". 
A simpler way is to use PyCharm, which quickly highlights and shows what needs to be installed. https://www.jetbrains.com/pycharm/download/?section=windows

All you need to do is adjust the configuration file(config.ini) with the values you require, and then the patches will be applied to the ESXi hosts.



Этот скрипт на Python 3 помогает обновлять одиночные ESXi хосты, к которым нельзя подключиться через vCSA. Я написал этот скрипт специально из-за необходимости автоматического обновления удалённых хостов без доступа к интернету и vCSA.

Чтобы начать использовать этот скрипт, нужно установить последнюю версию Python 3 и библиотеки из файла requirements.txt, затем запустить скрипт "esxi_patcher.py". Более простой способ — использовать PyCharm, который быстро подсветит и покажет, что нужно установить. https://www.jetbrains.com/pycharm/download/?section=windows

Вам всего лишь нужно настроить конфигурационный файл (config.ini), указав необходимые вам значения, и патчи будут применены к ESXi хостам.
