import pandas as pd
from datetime import *


def select_links():
    """
    extract users items who posted tweets from user_links
    :return: file csv
    """
    reader = pd.read_csv('deodorant_link.csv', header=None, names=['user_id', 'following_id'], iterator=True)
    loop = True
    chunkSize = 500000
    chunks = []
    while loop:
        try:
            chunk = reader.get_chunk(chunkSize)
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print("Iteration is stopped.")
    df = pd.concat(chunks, ignore_index=True)
    # a = df[df.duplicated()]
    a = pd.concat([df.drop_duplicates(), df.drop_duplicates(keep=False)]).drop_duplicates(keep=False)
    a.to_csv('deodorant_link_fin.csv', mode='a+', header=None, index=False)
    print('completed')


def dic_id_time():
    """
    create dict which include user id and their post time of tweet
    :return:user_id, post_time dictionary
    """
    user_tweet_list = []
    user_time_list = []
    with open('Tweets/R_DeodorantCancer.txt') as f2:
        for line, column in enumerate(f2):
            column = column.replace('\n', '')
            user_t_id, tweet_id, content, time = column.split('\t')[:]
            date_time = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
            user_tweet_list.append(user_t_id)
            user_time_list.append(date_time)
    user_tweet_list = list(map(int, user_tweet_list))
    dic = {'user_id': user_tweet_list, 'post_time': user_time_list}
    print("tweet_read_complete")
    return dic


def compare_time(time_u, time_f):
    """

    :param time_u: post time of user
    :param time_f: post time of following user
    :return: time_u later than time_f, true; else false
    """
    diff = time_u - time_f
    if diff.days > 0:
        return True
    else:
        return False


def remain_link(frame_u_time, frame_f_time):
    """
    compare earliest post time of user_id and following_id
    :param frame_u_time: data frame, post time of user_id
    :param frame_f_time: data frame, post time of following_id
    :return:remain link, true or not
    """
    min_u_time = min(frame_u_time['post_time'])
    min_f_time = min(frame_f_time['post_time'])
    res = compare_time(min_u_time, min_f_time)
    if res is True:
        return True
    else:
        return False


def determine_source(link_df, time_df):
    """

    :param link_df: remained link by remain_link func
    :param time_df: data frame, user_id, post_time
    :return: list of index which can be remained in link_df
    """
    index_list = []
    u_id_duplicated = link_df[link_df.user_id.duplicated(False)]
    user_duplicated = u_id_duplicated['user_id'].tolist()
    user_duplicated = set(user_duplicated)
    for user in user_duplicated:
        user_index = link_df[link_df.user_id == user].index.tolist()
        fol_id = link_df.iloc[user_index, [1]]
        fol_time_index = time_df[time_df.user_id == fol_id].index.tolist()
        fol_time = time_df.iloc[fol_time_index, [1]]
        fol_max_time = max(fol_time['post_time'])
        fol_max_id = time_df[time_df.post_time == fol_max_time]
        fol_index = link_df[link_df.following_id == fol_max_id].index.tolist()
        index_list.append(fol_index)
    return index_list


if __name__ == '__main__':
    # select_links()
    skip_count = 0
    dic = dic_id_time()
    df_time = pd.DataFrame(dic)
    df_time['user_id'].astype(int)
    reader = pd.read_csv('deodorant_link_fin.csv', header=None, names=['user_id', 'following_id'], iterator=True)
    loop = True
    chunkSize = 500000
    chunks = []
    while loop:
        try:
            chunk = reader.get_chunk(chunkSize)
            chunks.append(chunk)
        except StopIteration:
            loop = False
            print("Iteration is stopped.")
    df = pd.concat(chunks, ignore_index=True)
    link = pd.DataFrame(df)
    df_link_f1 = pd.DataFrame(columns=['user_id', 'following_id'])
    for row in link.itertuples():
        u_id = getattr(row, 'user_id')
        f_id = getattr(row, 'following_id')
        u_index = df_time[df_time.user_id == u_id].index.tolist()
        if u_index:
            u_time = df_time.iloc[u_index, [1]]
        else:
            skip_count += 1
            continue

        f_index = df_time[df_time.user_id == f_id].index.tolist()
        if f_index:
            f_time = df_time.iloc[f_index, [1]]
        else:
            skip_count += 1
            continue
        re = remain_link(u_time, f_time)
        if re is True:
            df_link_f1 = df_link_f1.append([{'user_id': u_id, 'following_id': f_id}], ignore_index=True)
        else:
            continue
    print(skip_count)
    # print(df_link_f1)
    # remain_index_list = determine_source(df_link_f1, df_time)
    # print(remain_index_list)


