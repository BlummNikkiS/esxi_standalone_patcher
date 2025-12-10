#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os
import sys
import time
import socket
import logging
import configparser
import paramiko
import ssl
import urllib3
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

# Отключаем предупреждения SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# PyVmomi импорты
try:
    from pyVim.connect import SmartConnect, Disconnect
    from pyVmomi import vim, vmodl
except ImportError as e:
    print(f"Ошибка: Не установлен pyVmomi. Установите: pip install pyvmomi")
    sys.exit(1)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'esxi_patcher_{time.strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Уменьшаем verbosity paramiko
logging.getLogger("paramiko").setLevel(logging.WARNING)
logging.getLogger("paramiko.transport").setLevel(logging.WARNING)


@dataclass
class ESXiHost:
    """Класс для хранения информации о хосте ESXi"""
    name: str
    ip: str
    username: str
    password: str
    ssh_port: int = 22
    api_port: int = 443


class ESXiStandalonePatcher:
    """Основной класс для патчинга standalone ESXi хостов"""

    def __init__(self, config_file: str = 'config.ini'):
        """Инициализация патчера"""
        self.config_file = config_file
        self.hosts: List[ESXiHost] = []
        self.patch_file: Optional[str] = None
        self.patch_name: Optional[str] = None
        self.timeout = 300
        self._load_config()

    def _load_config(self) -> None:
        """Загрузка конфигурации из файла"""
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f"Конфигурационный файл не найден: {self.config_file}")

        config = configparser.ConfigParser()
        config.read(self.config_file)

        # Настройка таймаута из settings (если указан)
        if 'settings' in config:
            try:
                self.timeout = int(config['settings'].get('timeout', str(self.timeout)))
            except Exception:
                logger.warning("Не удалось прочитать settings.timeout, используется значение по умолчанию")

        # Загрузка хостов
        for section in config.sections():
            if section.startswith('host_'):
                try:
                    host = ESXiHost(
                        name=config[section].get('name', section),
                        ip=config[section]['ip'],
                        username=config[section].get('username', 'root'),
                        password=config[section]['password'],
                        ssh_port=int(config[section].get('ssh_port', '22')),
                        api_port=int(config[section].get('api_port', '443'))
                    )
                except KeyError as e:
                    raise ValueError(f"Ошибка в конфиге: секция {section} не содержит обязательного поля {e}")
                self.hosts.append(host)
                logger.info(f"Загружен хост: {host.name} ({host.ip})")

        # Параметры патча
        if 'patch' in config:
            self.patch_file = config['patch'].get('patch_file', '').strip()
            if self.patch_file:
                self.patch_name = os.path.basename(self.patch_file)
                if not os.path.exists(self.patch_file):
                    logger.warning(f"Файл патча не найден локально: {self.patch_file}")

        if not self.hosts:
            raise ValueError("Не найдены хосты в конфигурации")

    def _connect_api(self, host: ESXiHost) -> Optional[vim.ServiceInstance]:
        """Подключение к API ESXi хоста"""
        try:
            context = ssl._create_unverified_context()

            si = SmartConnect(
                host=host.ip,
                user=host.username,
                pwd=host.password,
                port=host.api_port,
                sslContext=context
            )

            logger.info(f"Успешное подключение к API хоста {host.name}")
            return si

        except Exception as e:
            logger.error(f"Ошибка подключения к API {host.name}: {str(e)}")
            return None

    def _get_host_system(self, si: vim.ServiceInstance) -> Optional[vim.HostSystem]:
        """Получение объекта хоста из подключения"""
        try:
            content = si.RetrieveContent()
            container = content.viewManager.CreateContainerView(
                content.rootFolder, [vim.HostSystem], True
            )
            host = container.view[0] if container.view else None
            container.Destroy()
            return host
        except Exception as e:
            logger.error(f"Ошибка получения объекта хоста: {str(e)}")
            return None

    def is_host_in_cluster(self, host_obj: vim.HostSystem) -> bool:
        """Определение, находится ли хост в кластере"""
        try:
            parent = host_obj.parent
            if parent and hasattr(parent, 'name'):
                if isinstance(parent, vim.ClusterComputeResource):
                    logger.info(f"Хост находится в кластере: {parent.name}")
                    return True
                else:
                    logger.info(f"Хост НЕ в кластере (тип родителя: {type(parent).__name__})")
                    return False
            return False
        except Exception as e:
            logger.error(f"Ошибка определения кластерности хоста: {str(e)}")
            return False

    def enable_services_via_api(self, host_obj: vim.HostSystem) -> bool:
        """Включение служб TSM и TSM-SSH через API"""
        try:
            service_system = host_obj.configManager.serviceSystem
            if not service_system:
                logger.error("Не удалось получить систему служб")
                return False

            services_to_enable = ['TSM', 'TSM-SSH']
            enabled_services = []

            for service_name in services_to_enable:
                try:
                    service = None
                    for s in service_system.serviceInfo.service:
                        if s.key == service_name:
                            service = s
                            break

                    if not service:
                        logger.warning(f"Служба {service_name} не найдена на хосте")
                        continue

                    if not service.running:
                        try:
                            service_system.Start(service.key)
                            logger.info(f"Запуск службы {service_name}")
                        except Exception as e:
                            logger.warning(f"Не удалось запустить службу {service_name}: {e}")

                    if getattr(service, 'policy', None) != 'on':
                        try:
                            service_system.UpdateServicePolicy(service.key, 'on')
                            logger.info(f"Установка политики 'on' для службы {service_name}")
                        except Exception as e:
                            logger.warning(f"Не удалось установить политику 'on' для {service_name}: {e}")

                    enabled_services.append(service_name)

                except Exception as e:
                    logger.error(f"Ошибка включения службы {service_name}: {str(e)}")

            logger.info(f"Успешно включены службы: {enabled_services}")
            return len(enabled_services) > 0

        except Exception as e:
            logger.error(f"Критическая ошибка при включении служб: {str(e)}")
            return False

    def disable_services_via_api(self, host_obj: vim.HostSystem) -> bool:
        """Отключение служб TSM и TSM-SSH через API"""
        try:
            service_system = host_obj.configManager.serviceSystem
            if not service_system:
                logger.error("Не удалось получить систему служб")
                return False

            services_to_disable = ['TSM', 'TSM-SSH']
            disabled_services = []

            for service_name in services_to_disable:
                try:
                    service = None
                    for s in service_system.serviceInfo.service:
                        if s.key == service_name:
                            service = s
                            break

                    if not service:
                        logger.warning(f"Служба {service_name} не найдена на хосте")
                        continue

                    if getattr(service, 'running', False):
                        try:
                            service_system.Stop(service.key)
                            logger.info(f"Остановка службы {service_name}")
                        except Exception as e:
                            logger.warning(f"Не удалось остановить службу {service_name}: {e}")

                    if getattr(service, 'policy', None) != 'off':
                        try:
                            service_system.UpdateServicePolicy(service.key, 'off')
                            logger.info(f"Установка политики 'off' для службы {service_name}")
                        except Exception as e:
                            logger.warning(f"Не удалось установить политику 'off' для {service_name}: {e}")

                    disabled_services.append(service_name)

                except Exception as e:
                    logger.error(f"Ошибка отключения службы {service_name}: {str(e)}")

            logger.info(f"Успешно отключены службы: {disabled_services}")
            return len(disabled_services) > 0

        except Exception as e:
            logger.error(f"Критическая ошибка при отключении служб: {str(e)}")
            return False

    def wait_for_ssh(self, host: ESXiHost, timeout: int = 120) -> bool:
        """Ожидание доступности SSH службы"""
        logger.info(f"Ожидание доступности SSH на {host.name}...")

        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex((host.ip, host.ssh_port))
                sock.close()

                if result == 0:
                    logger.info(f"SSH доступен на {host.name}")
                    return True

            except Exception:
                pass

            time.sleep(5)

        logger.error(f"Таймаут ожидания SSH на {host.name}")
        return False

    def ssh_connect(self, host: ESXiHost) -> Optional[paramiko.SSHClient]:
        """Установка SSH подключения"""
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            client.connect(
                hostname=host.ip,
                port=host.ssh_port,
                username=host.username,
                password=host.password,
                timeout=30,
                banner_timeout=60
            )

            logger.info(f"SSH подключение установлено к {host.name}")
            return client

        except Exception as e:
            logger.error(f"Ошибка SSH подключения к {host.name}: {str(e)}")
            return None

    def ssh_execute_with_output(self, client: paramiko.SSHClient, command: str, timeout: int = 300) -> Tuple[
        bool, str, str]:
        """Выполнение команды по SSH с захватом вывода"""
        try:
            logger.info(f"Выполнение команды: {command}")

            stdin, stdout, stderr = client.exec_command(command, timeout=timeout)

            # Читаем вывод в реальном времени
            stdout_output = ""
            stderr_output = ""

            # Используем неблокирующее чтение
            import select

            # Пока процесс не завершил свою работу, читаем
            while not stdout.channel.exit_status_ready():
                # Проверяем доступные данные
                rlist, _, _ = select.select([stdout.channel], [], [], 1)

                if stdout.channel in rlist:
                    if stdout.channel.recv_ready():
                        data = stdout.channel.recv(4096).decode('utf-8', errors='ignore')
                        stdout_output += data
                        if data:
                            print(data, end='', flush=True)
                            logger.info(f"Вывод команды: {data.strip()}")

                # stderr обычно приходит тоже в stdout.channel выборке; но проверим stderr отдельно
                if stderr.channel.recv_stderr_ready():
                    err_data = stderr.channel.recv_stderr(4096).decode('utf-8', errors='ignore')
                    stderr_output += err_data
                    if err_data:
                        print(f"Ошибка: {err_data}", end='', flush=True)
                        logger.error(f"Ошибка команды: {err_data.strip()}")

            # Прочитать остатки после завершения
            stdout_output += stdout.read().decode('utf-8', errors='ignore')
            stderr_output += stderr.read().decode('utf-8', errors='ignore')

            exit_code = stdout.channel.recv_exit_status()

            # Логируем полный вывод
            if stdout_output.strip():
                logger.debug(f"Полный вывод команды '{command}':\n{stdout_output}")
            if stderr_output.strip():
                logger.debug(f"Полные ошибки команды '{command}':\n{stderr_output}")

            success = exit_code == 0
            return success, stdout_output.strip(), stderr_output.strip()

        except Exception as e:
            logger.error(f"Исключение при выполнении SSH команды '{command}': {str(e)}")
            return False, "", f"SSH execution error: {str(e)}"

    def ssh_execute(self, client: paramiko.SSHClient, command: str, timeout: int = 60) -> Tuple[bool, str]:
        """Выполнение команды по SSH (упрощенная версия)"""
        success, stdout, stderr = self.ssh_execute_with_output(client, command, timeout)

        if success:
            return True, stdout
        else:
            error_msg = stderr if stderr else "Unknown SSH execution error"
            return False, error_msg

    def find_boot_datastore(self, ssh_client: paramiko.SSHClient) -> Optional[str]:
        """Поиск загрузочного датастора"""
        success, output = self.ssh_execute(
            ssh_client,
            "esxcli storage filesystem list | grep -E '^/vmfs/volumes/[^ ]+' | head -1 | awk '{print $1}'"
        )

        if success and output and output.startswith('/vmfs/volumes/'):
            logger.info(f"Найден датастор: {output}")
            return output.strip()

        success, output = self.ssh_execute(
            ssh_client,
            "ls -d /vmfs/volumes/*/ 2>/dev/null | head -1"
        )

        if success and output:
            datastore = output.strip().rstrip('/')
            logger.info(f"Используем первый датастор: {datastore}")
            return datastore

        logger.error("Не удалось найти датастор")
        return None

    def copy_patch_via_scp(self, ssh_client: paramiko.SSHClient,
                           host: ESXiHost, datastore: str) -> bool:
        """Копирование патча через SCP"""
        if not self.patch_file or not os.path.exists(self.patch_file):
            logger.error(f"Файл патча не найден: {self.patch_file}")
            return False

        try:
            remote_path = f"{datastore}/{self.patch_name}"

            logger.info(f"Копирование {self.patch_file} -> {remote_path}")

            sftp = ssh_client.open_sftp()

            try:
                sftp.stat(datastore)
                logger.info(f"Датастор доступен: {datastore}")
            except Exception as e:
                logger.error(f"Датастор недоступен: {datastore}. Ошибка: {e}")
                sftp.close()
                return False

            sftp.put(self.patch_file, remote_path)

            stat = sftp.stat(remote_path)
            local_size = os.path.getsize(self.patch_file)

            if stat.st_size == local_size:
                logger.info(f"Файл успешно скопирован ({stat.st_size} байт)")
                sftp.close()
                return True
            else:
                logger.error(f"Размеры не совпадают: локальный={local_size}, удаленный={stat.st_size}")
                sftp.close()
                return False

        except Exception as e:
            logger.error(f"Ошибка копирования через SCP: {str(e)}")
            return False

    def enter_maintenance_mode(self, host_obj: vim.HostSystem, timeout: int = 0) -> bool:
        """Перевод хоста в режим обслуживания"""
        try:
            logger.info("Перевод хоста в режим обслуживания...")

            if host_obj.runtime.inMaintenanceMode:
                logger.info("Хост уже в режиме обслуживания")
                return True

            task = host_obj.EnterMaintenanceMode(timeout, False, None)
            self._wait_for_task(task)

            logger.info("Хост успешно переведен в режим обслуживания")
            return True

        except Exception as e:
            logger.error(f"Ошибка перевода в режим обслуживания: {str(e)}")
            return False

    def check_and_shutdown_vms(self, ssh_client: paramiko.SSHClient,
                               graceful_timeout: int = 180) -> bool:
        """
        Проверка и корректное выключение ВМ на standalone хосте.
        Логика:
         - Получаем список ВМ
         - Для каждой ВМ:
            * Если выключена — пропустить
            * Попытка graceful shutdown (vim-cmd vmsvc/power.shutdown)
            * Ждём до graceful_timeout секунд проверяя состояние
            * Если не выключилась — делаем power.off (force)
         - Возвращаем True если нет оставшихся запущенных ВМ, иначе False
        """
        try:
            # Получаем список всех ВМ
            success, output = self.ssh_execute(
                ssh_client,
                "vim-cmd vmsvc/getallvms | tail -n +2 | awk '{print $1}'"
            )

            if not success or not output.strip():
                logger.info("ВМ на хосте не найдены")
                return True

            vm_ids = [vid.strip() for vid in output.splitlines() if vid.strip()]
            logger.info(f"Найдено ВМ: {len(vm_ids)}")

            failed_vms: List[str] = []

            for vm_id in vm_ids:
                logger.info(f"Обработка ВМ ID: {vm_id}")

                # Получаем текущее состояние
                success, state_output = self.ssh_execute(
                    ssh_client,
                    f"vim-cmd vmsvc/power.getstate {vm_id}"
                )

                if not success:
                    logger.warning(f"Не удалось получить состояние ВМ {vm_id}, пропускаем force check")
                    # Попытаемся всё равно force power off как крайняя мера
                    success_force, _ = self.ssh_execute(ssh_client, f"vim-cmd vmsvc/power.off {vm_id}")
                    if not success_force:
                        failed_vms.append(vm_id)
                    continue

                if "Powered on" not in state_output:
                    logger.info(f"ВМ {vm_id} не запущена (состояние: {state_output})")
                    continue

                # Попытка graceful shutdown
                logger.info(f"ВМ {vm_id}: попытка graceful shutdown (vim-cmd vmsvc/power.shutdown {vm_id})")
                self.ssh_execute(ssh_client, f"vim-cmd vmsvc/power.shutdown {vm_id}")

                start = time.time()
                gracefully_stopped = False

                while time.time() - start < graceful_timeout:
                    time.sleep(5)
                    ok, st = self.ssh_execute(ssh_client, f"vim-cmd vmsvc/power.getstate {vm_id}")
                    if ok and "Powered off" in st:
                        logger.info(f"ВМ {vm_id}: корректно завершила работу (graceful)")
                        gracefully_stopped = True
                        break

                if gracefully_stopped:
                    continue

                # Если graceful не сработал — пробуем остановить через guest tools (если есть) — но т.к.
                # точный парсинг guest-tools может отличаться, сразу делаем принудительное выключение.
                logger.warning(f"ВМ {vm_id}: graceful shutdown не сработал, выполняем принудительное power.off")
                ok_force, _ = self.ssh_execute(ssh_client, f"vim-cmd vmsvc/power.off {vm_id}")

                if not ok_force:
                    logger.error(f"ВМ {vm_id}: Не удалось выполнить power.off")
                    failed_vms.append(vm_id)
                    continue

                # Небольшая пауза и проверка
                time.sleep(5)
                ok, st = self.ssh_execute(ssh_client, f"vim-cmd vmsvc/power.getstate {vm_id}")
                if not ok or "Powered off" not in st:
                    logger.error(f"ВМ {vm_id}: после force-off состояние: {st}")
                    failed_vms.append(vm_id)
                else:
                    logger.info(f"ВМ {vm_id}: успешно выключена принудительно")

            # Финальная проверка: есть ли еще powered on
            if failed_vms:
                logger.warning(f"Не удалось выключить ВМ: {failed_vms}")
                # Дополнительно проверим, остались ли вообще запущенные ВМ
                ok_all, all_states = self.ssh_execute(ssh_client, "vim-cmd vmsvc/getallvms | tail -n +2 | awk '{print $1}'")
                # Возвращаем False — у нас не все ВМ выключены
                return False

            logger.info("Все ВМ выключены")
            return True

        except Exception as e:
            logger.error(f"Ошибка при выключении ВМ: {str(e)}", exc_info=True)
            # Продолжаем работу даже при ошибке
            return False

    def start_vms_after_reboot(self, ssh_client: paramiko.SSHClient) -> bool:
        """Запуск ВМ после перезагрузки хоста (только для standalone)"""
        try:
            success, output = self.ssh_execute(
                ssh_client,
                "vim-cmd vmsvc/getallvms | tail -n +2 | awk '{print $1}'"
            )

            if not success or not output.strip():
                logger.info("ВМ на хосте не найдены")
                return True

            vm_ids = [vid.strip() for vid in output.splitlines() if vid.strip()]
            logger.info(f"Найдено ВМ для возможного запуска: {len(vm_ids)}")

            started_vms = 0
            failed = []

            for vm_id in vm_ids:
                success, state_output = self.ssh_execute(
                    ssh_client,
                    f"vim-cmd vmsvc/power.getstate {vm_id}"
                )

                if success and "Powered off" in state_output:
                    logger.info(f"Запуск ВМ ID: {vm_id}")
                    start_cmd = f"vim-cmd vmsvc/power.on {vm_id}"
                    success_start, _ = self.ssh_execute(ssh_client, start_cmd)

                    if success_start:
                        started_vms += 1
                        logger.info(f"ВМ {vm_id} запущена")
                    else:
                        logger.warning(f"Не удалось запустить ВМ {vm_id}")
                        failed.append(vm_id)

            if failed:
                logger.warning(f"Не удалось запустить следующие ВМ: {failed}")

            logger.info(f"Успешно запущено ВМ: {started_vms}")
            return True

        except Exception as e:
            logger.error(f"Ошибка при запуске ВМ: {str(e)}", exc_info=True)
            return False

    def install_patch_via_ssh(self, ssh_client: paramiko.SSHClient,
                              datastore: str) -> bool:
        """Установка патча через SSH с выводом в реальном времени"""
        if not self.patch_name:
            logger.error("Имя патча не определено")
            return False

        patch_path = f"{datastore}/{self.patch_name}"

        # Определяем тип патча по расширению файла
        if self.patch_name.endswith('.zip'):
            install_cmd = f"esxcli software vib install -d '{patch_path}' --no-sig-check"
        elif self.patch_name.endswith('.vib'):
            install_cmd = f"esxcli software vib install -v '{patch_path}' --no-sig-check"
        elif self.patch_name.endswith('.iso'):
            # Для ISO используем обновление профиля без указания конкретного имени
            install_cmd = f"esxcli software profile update -d '{patch_path}'"
        else:
            logger.error(f"Неизвестный тип патча: {self.patch_name}")
            return False

        logger.info(f"Установка патча: {install_cmd}")
        print(f"\n{'=' * 80}")
        print(f"НАЧИНАЕМ УСТАНОВКУ ПАТЧА:")
        print(f"Команда: {install_cmd}")
        print(f"{'=' * 80}\n")

        try:
            # Используем метод с выводом в реальном времени
            success, stdout, stderr = self.ssh_execute_with_output(ssh_client, install_cmd, timeout=1200)

            if success:
                print(f"\n{'=' * 80}")
                print("✅ ПАТЧ УСПЕШНО УСТАНОВЛЕН!")
                print(f"{'=' * 80}\n")
                logger.info(f"Патч успешно установлен")
                logger.info(f"Вывод установки: {stdout}")
                return True
            else:
                print(f"\n{'=' * 80}")
                print("❌ ОШИБКА УСТАНОВКИ ПАТЧА!")
                print(f"Ошибка: {stderr}")
                print(f"{'=' * 80}\n")
                logger.error(f"Ошибка установки патча: {stderr}")
                return False

        except Exception as e:
            logger.error(f"Исключение при установке патча: {str(e)}", exc_info=True)
            return False

    def verify_patch_installation(self, ssh_client: paramiko.SSHClient,
                                  patch_pattern: str = None) -> bool:
        """Проверка установки патча"""
        if not patch_pattern and self.patch_name:
            import re
            match = re.search(r'\d{8}', self.patch_name)
            if match:
                patch_pattern = match.group(0)

        if not patch_pattern:
            success, output = self.ssh_execute(ssh_client, "uname -a")
            if success:
                logger.info(f"Система загружена: {output[:100]}...")
                return True
            return False

        check_cmd = f"esxcli software vib list | grep -i {patch_pattern}"
        success, output = self.ssh_execute(ssh_client, check_cmd)

        if success and output:
            logger.info(f"Патч найден в системе: {output.strip()}")
            return True
        else:
            logger.warning(f"Патч с паттерном '{patch_pattern}' не найден в списке VIB")

            # Пробуем альтернативные методы проверки
            success, output = self.ssh_execute(ssh_client, "esxcli software vib list | tail -20")
            if success:
                logger.info(f"Последние установленные VIB: {output}")

            success, output = self.ssh_execute(ssh_client, "vmware -v")
            if success:
                logger.info(f"Версия ESXi: {output}")

            return False

    def cleanup_patch_file(self, ssh_client: paramiko.SSHClient,
                           datastore: str) -> bool:
        """Удаление файла патча"""
        patch_path = f"{datastore}/{self.patch_name}"

        logger.info(f"Удаление файла патча: {patch_path}")
        success, output = self.ssh_execute(ssh_client, f"rm -f '{patch_path}'")

        if success:
            logger.info("Файл патча удален")
            return True
        else:
            logger.warning(f"Не удалось удалить файл патча: {output}")
            return False

    def reboot_host(self, host_obj: vim.HostSystem) -> bool:
        """Перезагрузка хоста"""
        try:
            logger.info("Инициирование перезагрузки хоста...")

            # Отправляем команду перезагрузки
            task = host_obj.Reboot(force=False)
            logger.info("Команда перезагрузки отправлена")

            # Ждем начала выполнения задачи
            time.sleep(10)

            return True

        except Exception as e:
            logger.error(f"Ошибка при перезагрузке хоста: {str(e)}")
            return False

    def _wait_for_task(self, task, timeout: int = 1800):
        """Ожидание завершения задачи ESXI"""
        start_time = time.time()
        while task.info.state not in [vim.TaskInfo.State.success,
                                      vim.TaskInfo.State.error]:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Таймаут ожидания задачи: {timeout} сек.")
            time.sleep(5)

        if task.info.state == vim.TaskInfo.State.error:
            raise Exception(f"Ошибка задачи: {task.info.error}")

    def wait_for_host_reboot(self, host: ESXiHost, timeout: int = 900) -> bool:
        """Ожидание перезагрузки хоста"""
        logger.info(f"Ожидание перезагрузки хоста {host.name}...")

        start_time = time.time()
        host_went_down = False

        # Шаг 1: Ждем когда хост станет недоступен (начало перезагрузки)
        logger.info(f"1. Ожидание начала перезагрузки {host.name}...")
        print(f"\n⏳ Ожидание начала перезагрузки хоста {host.name}...")

        for i in range(60):  # Ждем до 5 минут (60 * 5 секунд)
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex((host.ip, 22))  # Проверяем SSH порт
                sock.close()

                if result != 0:
                    print(f"✅ Хост {host.name} начал перезагрузку (SSH недоступен)")
                    logger.info(f"Хост {host.name} начал перезагрузку (SSH недоступен)")
                    host_went_down = True
                    break
                else:
                    if i % 6 == 0:  # Каждые 30 секунд
                        elapsed = i * 5
                        print(f"   Хост еще доступен, ожидаем... ({elapsed} сек.)")
                        logger.info(f"Хост {host.name} еще доступен, ожидаем... ({elapsed} сек.)")
            except Exception as e:
                logger.debug(f"Ошибка проверки хоста: {str(e)}")
                # Это нормально во время перезагрузки

            time.sleep(5)

        if not host_went_down:
            print(f"⚠️ Хост {host.name} не стал недоступным, продолжаем...")
            logger.warning(f"Хост {host.name} не стал недоступным, продолжаем...")

        # Шаг 2: Ждем полного поднятия хоста
        print(f"\n⏳ Ожидание загрузки хоста {host.name}...")
        logger.info(f"2. Ожидание загрузки хоста {host.name}...")

        max_wait = 600  # Максимальное время ожидания: 10 минут
        wait_start = time.time()
        last_status_time = wait_start

        while time.time() - wait_start < max_wait:
            try:
                # Сначала проверяем SSH
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                result = sock.connect_ex((host.ip, 22))
                sock.close()

                if result == 0:
                    print(f"✅ SSH на хосте {host.name} доступен")
                    logger.info(f"SSH на хосте {host.name} доступен")

                    # Затем проверяем порт API (443)
                    time.sleep(15)  # Даем время для поднятия API

                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5)
                    result_api = sock.connect_ex((host.ip, 443))
                    sock.close()

                    if result_api == 0:
                        print(f"✅ API на хосте {host.name} доступен")
                        logger.info(f"API на хосте {host.name} доступен")
                        time.sleep(25)  # Дополнительное время для инициализации всех служб
                        print(f"✅ Хост {host.name} успешно загрузился!")
                        return True
                    else:
                        if time.time() - last_status_time > 30:
                            print(f"   API еще недоступен, ожидаем...")
                            last_status_time = time.time()
                else:
                    elapsed = int(time.time() - wait_start)
                    if elapsed % 30 == 0:  # Сообщаем каждые 30 секунд
                        print(f"   Хост еще не загрузился... ({elapsed} сек.)")
                        logger.info(f"Хост {host.name} еще не загрузился... ({elapsed} сек.)")

            except Exception as e:
                logger.debug(f"Ошибка проверки: {str(e)}")

            time.sleep(5)

        print(f"\n❌ Таймаут ожидания хоста {host.name}")
        logger.error(f"Таймаут ожидания хоста {host.name}")
        return False

    def exit_maintenance_mode(self, host_obj: vim.HostSystem) -> bool:
        """Выход из режима обслуживания"""
        try:
            logger.info("Вывод хоста из режима обслуживания...")

            if not host_obj.runtime.inMaintenanceMode:
                logger.info("Хост не в режиме обслуживания")
                return True

            task = host_obj.ExitMaintenanceMode(0)
            self._wait_for_task(task)

            logger.info("Хост успешно выведен из режима обслуживания")
            return True

        except Exception as e:
            logger.error(f"Ошибка вывода из режима обслуживания: {str(e)}")
            return False

    def test_connection(self, host: ESXiHost) -> Tuple[bool, str]:
        """Тестирование подключения к хосту"""
        try:
            print(f"\n🧪 Тестирование подключения к {host.name}...")

            # Тест SSH
            ssh_client = self.ssh_connect(host)
            if ssh_client:
                success, output = self.ssh_execute(ssh_client, "vmware -v")
                ssh_client.close()
                if success:
                    print(f"✅ SSH: OK - {output}")
                else:
                    print(f"❌ SSH: Ошибка - {output}")

            # Тест API
            api_conn = self._connect_api(host)
            if api_conn:
                host_obj = self._get_host_system(api_conn)
                if host_obj:
                    print(f"✅ API: OK - {host_obj.summary.config.product.fullName}")
                Disconnect(api_conn)

            return True, "Тест пройден"

        except Exception as e:
            return False, f"Ошибка теста: {str(e)}"

    def process_host(self, host: ESXiHost) -> Tuple[bool, str]:
        """Полный процесс патчинга для одного хоста"""
        host_start_time = time.time()
        print(f"\n{'=' * 80}")
        print(f"🚀 НАЧАЛО ОБРАБОТКИ ХОСТА: {host.name} ({host.ip})")
        print(f"{'=' * 80}")
        logger.info(f"\n{'=' * 60}")
        logger.info(f"НАЧАЛО ОБРАБОТКИ ХОСТА: {host.name} ({host.ip})")
        logger.info(f"{'=' * 60}")

        api_connection = None
        ssh_client = None

        try:
            # ШАГ 1: Подключение к API ESXi
            print("\n1. Подключение к API ESXi...")
            logger.info("1. Подключение к API ESXi...")
            api_connection = self._connect_api(host)
            if not api_connection:
                return False, "Ошибка подключения к API"

            host_obj = self._get_host_system(api_connection)
            if not host_obj:
                return False, "Ошибка получения объекта хоста"

            # Определение, находится ли хост в кластере
            is_clustered = self.is_host_in_cluster(host_obj)

            # ШАГ 2: Включение служб TSM и TSM-SSH через API
            print("2. Включение служб TSM и TSM-SSH...")
            logger.info("2. Включение служб TSM и TSM-SSH...")
            if not self.enable_services_via_api(host_obj):
                print("⚠️ Не удалось включить службы, но продолжаем...")
                logger.warning("Не удалось включить службы, но продолжаем...")

            # ШАГ 3: Ожидание доступности SSH
            print("3. Ожидание доступности SSH...")
            logger.info("3. Ожидание доступности SSH...")
            if not self.wait_for_ssh(host, timeout=120):
                print("⚠️ SSH не доступен, но продолжаем...")
                logger.warning("SSH не доступен, но продолжаем...")

            # ШАГ 4: Подключение по SSH
            print("4. Подключение по SSH...")
            logger.info("4. Подключение по SSH...")
            ssh_client = self.ssh_connect(host)
            if not ssh_client:
                return False, "Не удалось подключиться по SSH"

            # ШАГ 5: Определение загрузочного датастора
            print("5. Поиск загрузочного датастора...")
            logger.info("5. Поиск загрузочного датастора...")
            datastore = self.find_boot_datastore(ssh_client)
            if not datastore:
                return False, "Не удалось найти датастор"

            # ШАГ 6: Копирование патча на датастор
            if self.patch_file and os.path.exists(self.patch_file):
                print("6. Копирование файла патча...")
                logger.info("6. Копирование файла патча...")
                if not self.copy_patch_via_scp(ssh_client, host, datastore):
                    return False, "Не удалось скопировать патч"
            else:
                print("6. Пропуск копирования патча (файл не указан или не найден)")
                logger.info("6. Пропуск копирования патча (файл не указан или не найден)")

            # ШАГ 7: Обработка в зависимости от типа хоста
            if is_clustered:
                print("7. Хост в кластере: перевод в режим обслуживания...")
                logger.info("7. Хост в кластере: перевод в режим обслуживания...")
                if not self.enter_maintenance_mode(host_obj):
                    return False, "Не удалось перевести в режим обслуживания"

                print("8. Проверка состояния ВМ (ожидание миграции)...")
                logger.info("8. Проверка состояния ВМ (ожидание миграции)...")
                vm_result = self.check_and_shutdown_vms(ssh_client)
                if not vm_result:
                    print("⚠️ Не все ВМ выключены/мигрированы, но продолжаем...")
                    logger.warning("Не все ВМ выключены/мигрированы, но продолжаем...")
            else:
                print("7. Standalone хост: выключение всех ВМ...")
                logger.info("7. Standalone хост: выключение всех ВМ...")
                vm_result = self.check_and_shutdown_vms(ssh_client)
                if not vm_result:
                    print("⚠️ Не все ВМ удалось выключить, но продолжаем работу...")
                    logger.warning("Не все ВМ удалось выключить, но продолжаем работу...")

                print("8. Standalone хост: перевод в режим обслуживания...")
                logger.info("8. Standalone хост: перевод в режим обслуживания...")
                if not self.enter_maintenance_mode(host_obj):
                    return False, "Не удалось перевести в режим обслуживания"

            # ШАГ 9: Установка патча
            if self.patch_file and os.path.exists(self.patch_file):
                print("9. Установка патча...")
                logger.info("9. Установка патча...")
                if not self.install_patch_via_ssh(ssh_client, datastore):
                    return False, "Не удалось установить патч"
            else:
                print("9. Пропуск установки патча")
                logger.info("9. Пропуск установки патча")

            # ШАГ 10: Проверка установки
            print("10. Проверка установки патча...")
            logger.info("10. Проверка установки патча...")
            if not self.verify_patch_installation(ssh_client):
                print("⚠️ Не удалось подтвердить установку патча")
                logger.warning("Не удалось подтвердить установку патча")

            # ШАГ 11: Удаление файла патча
            if self.patch_file and os.path.exists(self.patch_file):
                print("11. Очистка файла патча...")
                logger.info("11. Очистка файла патча...")
                self.cleanup_patch_file(ssh_client, datastore)

            # ШАГ 12: Перезагрузка хоста
            print("12. Перезагрузка хоста...")
            logger.info("12. Перезагрузка хоста...")
            if not self.reboot_host(host_obj):
                return False, "Ошибка при перезагрузке"

            # Закрываем соединения перед перезагрузкой
            if ssh_client:
                ssh_client.close()
            if api_connection:
                Disconnect(api_connection)

            # ШАГ 13: Ожидание перезагрузки
            print("13. Ожидание перезагрузки хоста...")
            logger.info("13. Ожидание перезагрузки хоста...")
            if not self.wait_for_host_reboot(host, timeout=600):
                return False, "Хост не перезагрузился или недоступен после перезагрузки"

            # Ждем еще немного для полной инициализации
            print("Ждем инициализации всех служб...")
            time.sleep(30)

            # ШАГ 14: Повторное подключение
            print("14. Повторное подключение после перезагрузки...")
            logger.info("14. Повторное подключение после перезагрузки...")

            # Пробуем подключиться несколько раз
            max_retries = 5
            api_connection = None

            for attempt in range(max_retries):
                print(f"   Попытка подключения {attempt + 1}/{max_retries}...")

                api_connection = self._connect_api(host)
                if api_connection:
                    print("✅ Подключение к API восстановлено")
                    break
                else:
                    if attempt < max_retries - 1:
                        print(f"   Ожидание перед следующей попыткой...")
                        time.sleep(30)

            if not api_connection:
                return False, "Не удалось подключиться после перезагрузки"

            host_obj = self._get_host_system(api_connection)
            if not host_obj:
                return False, "Не удалось получить объект хоста после перезагрузки"

            # Подключаемся по SSH снова
            print("15. Повторное подключение по SSH...")
            logger.info("15. Повторное подключение по SSH...")
            ssh_client = self.ssh_connect(host)
            if not ssh_client:
                print("⚠️ Не удалось подключиться по SSH после перезагрузки")
                logger.warning("Не удалось подключиться по SSH после перезагрузки")

            # ШАГ 16: Выход из режима обслуживания
            print("16. Выход из режима обслуживания...")
            logger.info("16. Выход из режима обслуживания...")
            if not self.exit_maintenance_mode(host_obj):
                print("⚠️ Не удалось выйти из режима обслуживания")
                logger.warning("Не удалось выйти из режима обслуживания")

            # ШАГ 17: Запуск ВМ (только для standalone хостов)
            if not is_clustered and ssh_client:
                print("17. Запуск ВМ после перезагрузки...")
                logger.info("17. Запуск ВМ после перезагрузки...")
                if not self.start_vms_after_reboot(ssh_client):
                    print("⚠️ Не все ВМ удалось запустить")
                    logger.warning("Не все ВМ удалось запустить")

            # ШАГ 18: Отключение служб TSM и TSM-SSH (для безопасности)
            print("18. Отключение служб TSM и TSM-SSH...")
            logger.info("18. Отключение служб TSM и TSM-SSH...")
            if not self.disable_services_via_api(host_obj):
                print("⚠️ Не удалось отключить службы TSM/TSM-SSH")
                logger.warning("Не удалось отключить службы TSM/TSM-SSH")

            elapsed = int(time.time() - host_start_time)
            print(f"\n{'=' * 80}")
            print(f"✅ ХОСТ {host.name} УСПЕШНО ОБРАБОТАН за {elapsed} сек.")
            print(f"{'=' * 80}")
            logger.info(f"{'=' * 60}")
            logger.info(f"ХОСТ {host.name} УСПЕШНО ОБРАБОТАН за {elapsed} сек.")
            logger.info(f"{'=' * 60}")

            return True, "Успех"

        except Exception as e:
            print(f"\n❌ Критическая ошибка при обработке хоста {host.name}: {str(e)}")
            logger.error(f"Критическая ошибка при обработке хоста {host.name}: {str(e)}", exc_info=True)
            return False, f"Исключение: {str(e)}"

        finally:
            # Всегда закрываем соединения
            if ssh_client:
                try:
                    ssh_client.close()
                except:
                    pass

            if api_connection:
                try:
                    Disconnect(api_connection)
                except:
                    pass

    def run(self) -> bool:
        """Основной метод запуска"""
        print(f"\n{'*' * 80}")
        print(f"🚀 ЗАПУСК ESXi STANDALONE PATCHER")
        print(f"Количество хостов: {len(self.hosts)}")
        if self.patch_file:
            print(f"Патч: {self.patch_name}")
        print(f"{'*' * 80}\n")

        logger.info(f"\n{'*' * 60}")
        logger.info(f"ЗАПУСК ESXi STANDALONE PATCHER")
        logger.info(f"Количество хостов: {len(self.hosts)}")
        if self.patch_file:
            logger.info(f"Патч: {self.patch_name}")
        logger.info(f"{'*' * 60}\n")

        results = {}

        # Предварительное тестирование подключений
        if len(self.hosts) > 0:
            print("\n🧪 ПРЕДВАРИТЕЛЬНОЕ ТЕСТИРОВАНИЕ ПОДКЛЮЧЕНИЙ...")
            for host in self.hosts:
                success, message = self.test_connection(host)
                if not success:
                    print(f"❌ Тест подключения к {host.name} не пройден: {message}")
                    logger.error(f"Тест подключения к {host.name} не пройден: {message}")
                else:
                    print(f"✅ Тест подключения к {host.name} пройден")

            print("\n" + "-" * 80)

        for i, host in enumerate(self.hosts, 1):
            print(f"\n>>> Обработка хоста {i}/{len(self.hosts)}: {host.name}")
            logger.info(f"\n>>> Обработка хоста {i}/{len(self.hosts)}: {host.name}")

            success, message = self.process_host(host)
            results[host.name] = (success, message)

            if i < len(self.hosts):
                pause = 30
                print(f"\nПауза {pause} сек. перед следующим хостом...")
                logger.info(f"Пауза {pause} сек. перед следующим хостом...")
                time.sleep(pause)

        print(f"\n{'*' * 80}")
        print("📊 РЕЗУЛЬТАТЫ ВЫПОЛНЕНИЯ:")
        print(f"{'*' * 80}")
        logger.info(f"\n{'*' * 60}")
        logger.info("РЕЗУЛЬТАТЫ ВЫПОЛНЕНИЯ:")
        logger.info(f"{'*' * 60}")

        success_count = 0
        fail_count = 0

        for host_name, (success, message) in results.items():
            status = "✅ УСПЕХ" if success else "❌ ОШИБКА"
            print(f"{host_name}: {status} - {message}")
            logger.info(f"{host_name}: {'УСПЕХ' if success else 'ОШИБКА'} - {message}")

            if success:
                success_count += 1
            else:
                fail_count += 1

        print(f"\n📈 Итого: Успешно - {success_count}, С ошибками - {fail_count}")
        logger.info(f"\nИтого: Успешно - {success_count}, С ошибками - {fail_count}")

        return fail_count == 0


def main():
    """Точка входа в программу"""
    try:
        if not os.path.exists('config.ini'):
            print("❌ Конфигурационный файл config.ini не найден!")
            print("\nСоздайте файл config.ini со следующей структурой:")
            print("\n[settings]")
            print("timeout = 300")
            print("\n[patch]")
            print("# patch_file = C:\\path\\to\\ESXi-patch.zip  # опционально")
            print("\n[host_esxi01]")
            print("name = ESXi-01")
            print("ip = 192.168.1.101")
            print("username = root")
            print("password = your_password")
            print("ssh_port = 22")
            print("api_port = 443")

            # Создаем пример конфига
            with open('config.ini', 'w') as f:
                f.write("""[settings]
timeout = 300

[patch]
# Укажите путь к файлу патча (опционально)
# patch_file = C:\\path\\to\\ESXi650-202403001.zip

[host_esxi01]
name = ESXi-01
ip = 192.168.1.101
username = root
password = your_password
ssh_port = 22
api_port = 443
""")
            print("\n✅ Создан пример конфигурационного файла config.ini")
            print("Отредактируйте его и запустите скрипт снова.")
            sys.exit(1)

        patcher = ESXiStandalonePatcher('config.ini')
        success = patcher.run()

        if success:
            print("\n✅ Все хосты успешно обработаны!")
            sys.exit(0)
        else:
            print("\n❌ Были ошибки при обработке хостов")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\n⚠️  Прервано пользователем")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Ошибка запуска: {str(e)}", exc_info=True)
        print(f"\n❌ Критическая ошибка: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
