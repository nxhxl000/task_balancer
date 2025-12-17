# Task Balancer

## Быстрый старт

### 1) Клонирование репозитория

Склонируй проект и перейди в папку:

```bash
git clone https://github.com/nxhxl000/task_balancer.git
cd task_balancer
```

---

### 2) Создание и активация окружения (Conda)

Все зависимости проекта описаны в файле `environment.yml`.

Создай окружение:

```bash
conda env create -f environment.yml
```

Активируй окружение:

```bash
conda activate task-balancer
```

---

### 3) Обновление окружения

Если `environment.yml` изменился (добавились/обновились зависимости), обнови окружение:

```bash
conda env update -f environment.yml --prune
```

И снова активируй (если ты в новом терминале):

```bash
conda activate task-balancer
```

`--prune` удаляет пакеты, которых больше нет в `environment.yml`, чтобы окружение не “захламлялось”.

---

### 4) Как добавлять новые библиотеки

Любые новые зависимости **обязательно** добавляй в `environment.yml` (в `dependencies:` или в секцию `pip:`).

После изменений обнови окружение:

```bash
conda env update -f environment.yml --prune
```

---

### 5) Как закоммитить изменения в репозиторий

Добавь изменения и отправь их в репозиторий:

```bash
git add environment.yml
git commit -m "Update conda environment dependencies"
git push
```

### 6) Создай файл .env в корне проекта и добавь в него строку подключения Neon (PostgreSQL):

Пример .env представлен в .env.example 

```bash
DATABASE_URL=postgresql://USER:PASSWORD@HOST/dbname?sslmode=require
```

### 6) Запуск локальных Demo тестов оркестратора:

Запускаем Оркестратор в отдельном терминале (два режима запуска):

Реальный режим (сервис):

```bash
python -m app.orchestrator.run --mode real
```

Демо режим (завершится, если TIME сек нет задач):

```bash
python -m app.orchestrator.run --mode demo --idle-exit-seconds <TIME>
```

Команда для запуска демо прогона:

```bash
python -m scripts.run_demo --tasks 10 --sleep 2 --priority 1000 --timeout 180
#--tasks  - сколько задач создать (enqueue) в таблицу tasks для этого прогона (run_id)
#--sleep  - Сколько секунд будет “выполняться” каждая demo-задача (это значение попадает в payload.sleep_s)
#--priority - Приоритет задач в очереди (поле priority в БД).
#--timeout 180 - Максимальное время (в секундах), которое run_demo.py будет ждать завершения всех задач этого run_id
```

Команда для удаления демо задач из БД:

```bash
python -m scripts.db_reset_run --run-id <RUN ID> --yes
```

Команда для приведения реальных задач к исходному состоянию (если их тоже затронул Demo прогон):

```bash
python -m scripts.reset_real_tasks --only-backend local --yes
```

### 6) Перезапуск ВМ для BOINC сервера:

При остановке и звпуске ВМ меняется публичный ID
Необходимо на BOINC сервере изменить файл .env и указать там актуальный ID ВМ:

```bash
# the URL the server thinks its at
URL_BASE=http://158.160.21.144
```

---

### frontend

Создай файл .env в корне проекта и добавь в него строку расположения API:

```bash
VITE_TASK_API_URL=http://127.0.0.1:8000
```

Билд + запуск

```bash
cd frontend
npm install
npm run dev
```

Откройте:  

- API: <http://localhost:8000/docs>  
- фронт: <http://localhost:5173> — нажмите «Загрузить список», появятся записи из БД.

---

## ✅ Быстрый старт (после клонирования)

```bash
# Backend # FastApi
conda activate task-balancer
uvicorn fast-app-api.app:app --reload --port 8000

# Frontend
cd frontend
npm install
npm run dev
```
