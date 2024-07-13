import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

sample_log1 = "This is the first log"
sample_log2 = "This is the second log"
sample_log3 = "Pen pineapple apple pen"

sample_logs = [sample_log1, sample_log2, sample_log3]

count_vectorizer = CountVectorizer()
bag_of_words_matrix = count_vectorizer.fit_transform(sample_logs)

feature_names = count_vectorizer.get_feature_names_out()
feature_names

bag_of_words_data_frame = pd.DataFrame(
    bag_of_words_matrix.toarray(),
    columns=feature_names,
    index=["log1", "log2", "log3"],
)
bag_of_words_data_frame

cosine_similarity = cosine_similarity(bag_of_words_matrix)

similarity_data_frame = pd.DataFrame(
    cosine_similarity,
    index=["log1", "log2", "log3"],
    columns=["log1", "log2", "log3"],
)
similarity_data_frame
