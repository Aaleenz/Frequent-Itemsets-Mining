# Frequent-Itemsets-Mining
This repository contains the code and documentation for the Fundamental of Big Data Analytics course assignment focused on analyzing Amazon metadata. In this assignment, we performed preprocessing on the metadata and implemented various algorithms using Apache Kafka for data streaming.
# Project Overview

The project comprises the following components:
## Metadata Preprocessing: 
Initial preprocessing of Amazon metadata to prepare it for further analysis.
## Apache Kafka Integration: 
Utilized Apache Kafka to stream the preprocessed data to multiple consumers.
## Consumers:
Apriori Algorithm Implementation: Implemented the Apriori algorithm to find frequent itemsets in the streamed data.
PCY Algorithm Implementation: Implemented the PCY (Park Chen Yu) algorithm to efficiently mine frequent itemsets.
Brand Association Analysis: Identified and printed the brands most commonly associated with products viewed or bought by users.

# Why This Approach?

We chose this approach for several reasons:

    Scalability: Apache Kafka provides distributed streaming capabilities, allowing us to scale our system horizontally to handle large volumes of data efficiently.

    Real-time Analytics: By using Kafka for data streaming, we can perform real-time analytics on the incoming data. This is crucial for applications requiring timely insights, such as analyzing user behavior on e-commerce platforms.

    Modularity: Implementing different algorithms as separate consumers allows for modular development and easier maintenance. Each consumer can focus on a specific analytics task without affecting the others.

    Algorithm Variety: By implementing both classic (Apriori) and more optimized (PCY) algorithms, we can compare their performance and effectiveness in mining frequent itemsets from the data stream.

    Practical Application: Analyzing user-product interactions and identifying common brand associations has practical applications in e-commerce for targeted marketing, product recommendations, and inventory management.
