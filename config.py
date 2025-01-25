#PYTHON VERSION == 3.12.4

#API Ключ для бота телеграм
API_TOKEN = '7470564935:AAEPSpK-Vea4EbcpfliTIlQzfvrRF7mSxNk'


# Путь к файлу с API ключами Passivbot
API_KEYS_DIR = "api_keys" # Название папки с файлом апи ключей
API_KEYS_FILE = "api_keys.json" # Название файла с апи ключами Passivebot
API_KEYS_DEACTIVATED_FILE = "api_keys_deactivated.json" # Название деактивированного файла с апи ключами


# Команды для завершения работы и запуска скрипта в tmux
deactivate_command = "tmux send-keys -t your-session-name 'your-deactivate-command' C-m"
restore_command = "tmux send-keys -t your-session-name 'your-restore-command' C-m"
