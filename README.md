# erad2016-streamprocessing
Stream Processing Application for Sentiment Analysis with Storm and Spark @ ERAD 2016

## Sentiment Analysis

The **Sentiment Analysis** application uses a simple NLP technique for calculating the sentiment of sentences, consisting of counting positive and negative words adn using the difference to indicate the polarity of the sentence.

The application receives a stream of *tweets* in the JSON format, each *tweet* corresponds to an event that has to be parsed in order to extract the relevant fields, in this case the *ID* of the tweet, the *language* and the *message*.

<img src="https://raw.githubusercontent.com/mayconbordin/erad2016-streamprocessing/master/dataflow.png" /*height="300"*/width="100%" />

After being parsed, the tweets are filtered, removing those that have been written in a language that is not supported by the application. By default only the English language is supported, to extend the support the operators that load the negative/positive list of words would have to switch lists between languages for each new event.

Next, the tweets go through the *Stemmer*, which removes *stop words* from the message, which are words that usually don't carry sentiment, and thus are irrelevant for the next steps in the application.

With the tweets filtered and cleaned, the stream is duplicated to two operators that will count the number of occurrences of positive and negative words. Using the ID of the tweet these two streams will be joined, creating a new event with both the positive and the negative counters. The next operator will then calculate sentiment of the tweet, which will be positive if the number of occurrences of positive words is greater than the negative ones, or negative otherwise.

## Implementations

The **Sentiment Analysis** application has been implemented in the following platforms:

  - [Storm](https://github.com/mayconbordin/erad2016-streamprocessing/tree/master/sentimentanalysis-storm)
  - [Spark Streaming](https://github.com/mayconbordin/erad2016-streamprocessing/tree/master/sentimentanalysis-spark)

## More Applications

For more applications for Storm see [storm-applications](https://github.com/mayconbordin/storm-applications) and [trident-examples](https://github.com/mayconbordin/trident-examples).
