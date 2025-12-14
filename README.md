## Быстрый старт

### Клонирование репозитория

```bash
git clone https://github.com/nxhxl000/task_balancer.git
cd task_balancer

Создание виртуального окружения и его обновление

Библиотеки и фреймворки окружения описаны в environment.yml

Команды для создания и активации через conda:

```bash
conda env create -f environment.yml
conda activate vkr-fl-balancer


Если environment.yml изменился (добавились/обновились зависимости), обнови окружение так:

```bash
conda env update -f environment.yml --prune
conda activate vkr-fl-balancer

--prune удаляет пакеты, которые больше не перечислены в environment.yml, чтобы окружение не “захламлялось”.


Все новые зависимости нужно фиксировать в environment.yml.

Рекомендуемый порядок:

1.Добавь библиотеку в environment.yml (в dependencies: или в секцию pip:).

2.Примени изменения:

```bash
conda env update -f environment.yml --prune

3.Закоммить изменения environment.yml в репозиторий.
```bash
git add environment.yml
git commit -m "Update conda environment dependencies"
git push