import pandas as pd #import library
import numpy as np
import sys
import os

from settings.channel_list import CHANNEL_LIST


# data from raw -> deriving dt -> classification -> filter -> time delta founding -> saving into data directory
# directory path
AppPath = sys.path[0]
data_folder = AppPath + '/data'
rawdata_folder = AppPath + "/data_raw"

# printing the ways
print(os.listdir(rawdata_folder))
print(data_folder)
print(AppPath)
# to raw data folder
os.chdir(rawdata_folder)


# def part

# operation commands
south = ['Вінницька','Херсонська', 'Кропивницька', 'Миколаївська', 'Одеська', 'Херсонська']
north = ['Сумська', 'Житомирська', 'Київська', 'Полтавська', 'Черкаська', 'Чернігівська']
west = ['Волинська', 'Чернівецька', 'Івано-Франківська', 'Львівська', 'Рівненська',
        'Тернопільська', 'Хмельницька', 'Закарпаття']
east = ['Запорізька', 'Дніпровська', 'Харьківська', 'Донецька']


def operation_command(region):
    if region in south:
        return 'South'
    if region in north:
        return 'North'
    if region in west:
        return 'West'
    if region in east:
        return 'East'
    return None


# week day
def week_day(day):
    if pd.isna(day):
        return None
    if day == 0:
        return 'Monday'
    if day == 1:
        return 'Tuesday'
    if day == 2:
        return 'Wednesday'
    if day == 3:
        return 'Thursday'
    if day == 4:
        return 'Friday'
    if day == 5:
        return 'Saturday'
    if day == 6:
        return 'Sunday'


# month name
def month_name(month):
    if pd.isna(month):
        return None
    if month == 1:
        return 'January'
    if month == 2:
        return 'February'
    if month == 3:
        return 'March'
    if month == 4:
        return 'April'
    if month == 5:
        return 'May'
    if month == 6:
        return 'June'
    if month == 7:
        return 'July'
    if month == 8:
        return 'August'
    if month == 9:
        return 'September'
    if month == 10:
        return 'October'
    if month == 11:
        return 'November'
    if month == 12:
        return 'December'


# the datetime column
def splitting_datetime(raw_df):
    df = raw_df.rename(columns={'date': 'datetime'})
    # splitting
    df['time'] = df['datetime'].dt.time
    df['hour'] = df['datetime'].dt.hour
    df['date'] = df['datetime'].dt.date
    df['day'] = df['datetime'].dt.day
    df['month_num'] = df['datetime'].dt.month
    df['month'] = df['month_num'].apply(lambda x: month_name(x))
    df['war_day'] = df['datetime'].dt.dayofyear - 31 - 23
    df['war_week'] = (df['datetime'].dt.dayofyear - 31 - 23) // 7
    df['weekday_num'] = df['datetime'].dt.dayofweek
    df['weekday'] = df['weekday_num'].apply(lambda x: week_day(x))
    return df


# column for checking is the end of warning
start_warning = ('оголошено тривогу',
                     'повітряна тривога',
                     'повітряна тривога',
                     'увага повітряна',
                     'в укриття',
                     'лунає сирена',
                     'лунають сирени',
                     'воздушная тревога',
                     'ТРЕВОГА ПО ВСЕЙ ОБЛАСТИ'.lower(),
                     'УСЯ ОБЛАСТЬ ТРИВОГА'.lower(),
                     'ПОВТОРНА ПОВІТРЯНА НЕБЕЗПЕКА'.lower(),
                     'Київ ТРИВОГА'.lower(), 'Києві сирени'.lower(), 'Тривога  Києві'.lower(),
                     'залишайтесь в укритті', 'загроза авіаудар',
                     'вінниця сирени', 'тривога ще триває',
                     'вінничина тривога',
                     'житомир тривога',
                     'житомир сирени',
                     'чернігів тривога',
                     'усім в укриття',
                     'в укрытие',
                     'воздушная тревога продолжается',
                     'тривога знову'
                     )
end_warning = ('відбій тривоги',
                   'відбій повітряної тривоги',
                    'запоріжжя відбій',
                    'отбой тревоги',
                    'відміна тривоги',
                    'тривогу скасовано',
                    'ВІДБІЙ'.lower(),
                    'ВІДБІЙ ПОВІТРЯНОЇ НЕБЕЗПЕКИ'.lower(),
                    'київ відбій',
                    'можна покидати укриття',
                    'загроза минула',
                    'тривогу скасовано',
                    'тривога відбій'
                )
start_black_list = ['шановні цивільні', 'канал створено для оперативного інформування цивільного населення',
                    "РОЗ'ЯСНЕННЯ ПО СИРЕНАХ".lower(), 'надаємо офіційні роз‘яснення', 'приводу роботи сигналів сирен',
                    'актуальна інформації про сирени повітряної тривоги', 'розповідайте усім',
                    'якщо в чаті ви бачите повідомлення', 'може зараз бути навчальна сирена',
                    'сповіщатиме про небезпеку у конкретному регіоні україни можна завантажити'
                    'інформація не про тривогу','рекомендуємо встановити офіційний додаток',
                    "лишаються проблеми зі зв'язком", 'сталася помилка']
end_black_list = ['актуальна інформації про сирени повітряної тривоги',
                  'на даний момент актуальної інформації',
                  'якщо в чаті ви бачите повідомлення',
                  'цей канал створений для попередження',
                  'лагодяться сирени',
                  'це спрацювали приватні сирени', 'сирена це попередження',
                  'скасовано',
                  "РОЗ'ЯСНЕННЯ ПО СИРЕНАХ".lower(),
                  'поки немає офіційного повідомлення',
                  'не покидайте укриття', 'не виходьте з укриттів',
                  'звертаюсь до вас з важливим повідомленням',
                  'шановні учасники нашого співтовариства',
                  'розповідайте усім', 'жодної тривоги в місті не було', 'сталася помилка'
                  ]
intersection_func = set.intersection
spec_chars = '✅ ! 📢 ✊ 🚨 @ # $ % ^ & * ❗ ‼ , ❗ ‼ 🙏 . \ - 🙏‼ {\n} _ ❕ 💥 " '.split()


# func works in Message column, clear -> classify the message to end or start of warning
def check_end(old_message):
    if pd.isna(old_message):
        return None
    # cleaning the message from special chars
    # '@String!' -> 'String'
    message = old_message.lower()
    message = ''.join(map(str, list(filter(lambda x: x not in spec_chars, message))))
    message = ' '.join(map(str, [word.strip() for word in message.strip('️').split()]))
    message = message.strip(' ')

    # loop for checking the message
    for warning in start_warning:
        # deriving the message and the warnings to the sets
        if len(set.intersection(set(message.split()), set(warning.split()))) >= 2 or (warning in message):
            if len(list(filter(lambda x: x in message, start_black_list))) < 1:
                return 0
        else:
            for end in end_warning:
                if len(set.intersection(set(message.split()), set(end.split()))) >= 2 or (end in message):
                    if len(list(filter(lambda x: x in message, end_black_list))) < 1:
                        return 1
    return None


# func for deleting the duplicate values
def check_reduntant(raw_df): # df - pd.Dataframe
    # input
    raw_index = raw_df.index
    new_index = np.arange(0, raw_index.shape[0])
    df = raw_df.copy().reset_index()
    df.index = new_index
    # random list for id
    rng = np.random.default_rng()
    random_list = np.arange(0, raw_index.shape[0], dtype=int)
    rng.shuffle(random_list)
    # creating new columns for checking
    df['unique_warning'] = None
    df['last_end'] = None
    print('start_func')
    # loop per every row of df
    for row in range(df.shape[0]):
        # selecting any random number for id the row
        rand_int = random_list[0]
        random_list = np.delete(random_list, 0, 0)
        # checking that message is start of warning
        if df['is_warning'].iloc[row] == 1:
            df.loc[row, 'unique_warning'] = rand_int
            for i in range(1, row+1):
                if df['is_warning'].iloc[row - i] == 1:
                    df.loc[row - i, 'unique_warning'] = rand_int
                else:
                    break
        # checking that message is the end of warning
        # if first row is warning it will be deleted
        if df['is_warning'].iloc[row] == 0:
            df.loc[row, 'last_end'] = rand_int
            for i in range(1, row+1):
                if df['is_warning'].iloc[row - i] == 0:
                    df.loc[row - i, 'last_end'] = rand_int
                else:
                    break
        # work with nan outliers
        if pd.isna(df['is_warning'].iloc[row]):
            continue
    print(df[['id', 'is_end', 'unique_warning','is_warning', 'last_end']].head(200).values)
    # founding the min start of warning
    unique_warning = df[['unique_warning', 'id']].groupby(by=['unique_warning']).min()
    unique_warning = unique_warning.rename(columns={'id': 'min_id'})
    # founding the latest end of warning
    last_end = df[['last_end', 'id']].groupby(by=['last_end']).max()
    last_end = last_end.rename(columns={'id': 'max_id'})
    # joining the both to major dataset
    df = pd.merge(df.set_index('unique_warning'), unique_warning, how='left', left_index=True, right_index=True)
    df = df.reset_index('unique_warning')
    df = pd.merge(df.set_index('last_end'), last_end, how='left', left_index=True, right_index=True)
    df = df.reset_index('last_end')
    df = df.sort_values(by='id', ascending=False)
    # converting into integer
    df['max_id'] = pd.to_numeric(df['max_id'], downcast='integer')
    df['min_id'] = pd.to_numeric(df['min_id'], downcast='integer')
    # checking if id is max or min id
    print(unique_warning.shape[0], last_end.shape[0])
    df['unique_warning'] = df['id'] == df['min_id']
    df['last_end'] = df['id'] == df['max_id']
    print('end_func')
    return df


# function that create a new column with difference of time
def time_delta(raw_df):
    # input
    df = raw_df.copy()
    # new vars
    first_row = 0
    # loop for founding the row from which we start to calculate the time delta
    for row in range(df.shape[0]):
        if not pd.isna(df['is_end'].iloc[row]):
            if df['is_end'].iloc[row] == 1:
                first_row = row
                break
            if df['is_end'].iloc[row] == 0:
                first_row = row
    print(df.shape[0])
    datetime_df = df[['id', 'datetime', 'is_end', 'last_end', 'unique_warning']][first_row:].copy()
    # print(datetime_df)
    datetime_df = datetime_df.set_index('id').copy()
    # deriving into two df -> 1. with starts, 2. - with ends
    df_start = pd.to_datetime(datetime_df[datetime_df['unique_warning'] == True]['datetime'])
    df_end = pd.to_datetime(datetime_df[datetime_df['last_end'] == True]['datetime'])
    # deleting the first row if it is last_end
    if datetime_df[datetime_df['last_end'] == True].index.min() <= \
       datetime_df[datetime_df['unique_warning'] == True].index.min():
        df_end = df_end.drop([datetime_df[datetime_df['last_end'] == True].index.min()], axis=0)

    print('#',df_end.index.shape, df_end.index)
    print('&', df_start.index.shape, df_start.index)
    # indexes should be equal for both
    df_start.index = df_end.index
    # delta
    df_delta = pd.to_numeric((df_end - df_start).dt.seconds // 60)
    df_delta.name = 'time_delta_min'
    # merging
    df = pd.merge(df.set_index('id'), df_delta, how='left', left_index=True, right_index=True)
    return df


# Major Part
os.chdir(rawdata_folder)
i = 0 # if first iteration we create the file, if > 0 then we update
mode = 'w' # mode for writing into file
header = True
# list of columns for dropping
drop_columns = ['out', 'mentioned','media_unread', 'silent', 'post', 'from_scheduled', 'legacy',
                'edit_hide', 'pinned', 'from_id', 'fwd_from', 'via_bot_id', 'reply_to', 'peer_id',
                'media', 'reply_markup', 'entities', 'views', 'forwards', 'replies',
                'edit_date', 'post_author', 'grouped_id', 'restriction_reason']
# loop per every file
for file in os.listdir(rawdata_folder):
    # checking that file is necessary for analysis
    if file.replace('.json', '') not in CHANNEL_LIST.values():
        print(f'{file}')
        continue
    # 0. reading the raw data
    raw_df = pd.read_json(file)
# 1. dropping the columns
    df = raw_df.drop(columns=drop_columns).copy()
# 2. adding the region
    channel = file.replace('.json', '')
    # taking the name of region
    region = ''.join(map(str, [city for city, item in CHANNEL_LIST.items() if item == channel]))
    print(region)
    # exception any region from dataset
    df['region'] = region
    df['command_center'] = df['region'].apply(lambda x: operation_command(x))
    if region == 'Херсонська':
        print('SKIPPED')
        continue
# 2. splitting datetime
    df_splt_dt = splitting_datetime(df)
# 3. marking the message as start of warning and the end of warning
    df_splt_dt['is_end'] = 0
    df_splt_dt['is_end'] = df_splt_dt['message'].apply(lambda x: check_end(x))
    # opposite to is_end column
    df_splt_dt['is_warning'] = (df_splt_dt['is_end'] - 1) ** 2
# 4. cleaning the data from nans
    df_dropna = df_splt_dt[pd.isna(df_splt_dt['is_end']) == False]
# 5.for checking the redundant warnings adds new column is_reduntant
    df_chk_reduntant = check_reduntant(df_dropna)
# 6. founding the time delta
    df_time_delta = time_delta(df_chk_reduntant)
    # changing the directory
    os.chdir(data_folder)
# 7. writing into file
    if i == 1:
        mode = 'a'
        header = False
    df_time_delta.to_csv('data.csv', sep='~', mode=mode, header=header)
    # updating the i
    i += 1
    os.chdir(rawdata_folder)


