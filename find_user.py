import pandas as pd
import datetime


def read_tweets():
    """
    读取twitter生成user列表
    :return: user列表
    """
    user_tweet_list = []
    with open('Tweets/R_DeodorantCancer.txt') as f2:
        for line, column in enumerate(f2):
            column = column.replace('\n', '')
            user_t_id, tweet_id, content, time = column.split('\t')[:]
            user_tweet_list.append(user_t_id)
    user_tweet_list = list(map(int, user_tweet_list))
    user_tweet_list = set(user_tweet_list)  # 去除重复用户id
    print("tweet_read_complete")
    return user_tweet_list


def read_links(tweet_user_list):
    """
    抽取出
    :param tweet_user_list: 上一个方法生成的list
    :return:
    """
    reader = pd.read_csv('links.csv', header=None, names=['user_id', 'following_id'], iterator=True)
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

    usr_l = []
    usr_r = []
    user_index = []
    for user_id in tweet_user_list:
        # usr_l = df[df['user_id'] == user_id].index.tolist()
        # usr_r = df[df['following_id'] == user_id].index.tolist()
        a = df[(df.user_id == user_id) | (df.following_id == user_id)]
        a.to_csv('deodorant_link.csv', mode='a+', header=None, index=False)

    # for usr_l_index in usr_l:
    #     for usr_r_index in usr_r:
    #         if usr_l_index == usr_r_index:
    #             user_index.append(usr_l_index)
    # return user_index


if __name__ == "__main__":
    start = datetime.datetime.now()

    user_id_from_twitter = read_tweets()  # 生成user列表
    read_links(user_id_from_twitter)
    print('process complete')

    end = datetime.datetime.now()
    print(end - start)

