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

