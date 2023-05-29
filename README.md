# Poverty Prediction Using Urban Infrastructure Data

# Research Question
Addressing neighborhood poverty poses a significant challenge due to the dearth of comprehensive and timely data that captures the multifaceted nature of poverty. Traditional poverty assessment approaches often rely on limited demographic data, neglecting the impact of urban infrastructure on neighborhood dynamics. This research aims to transcend these constraints by exploring accessible urban infrastructure information to provide a more nuanced comprehension of neighborhood poverty.

The primary research question driving this project is: How can urban infrastructure information be leveraged to predict poverty? To acquire urban infrastructure data, two principal sources are explored: bikeshare data and satellite imagery. The MapBox API facilitates the collection of satellite imagery, enabling the analysis of average house density within each census tract. The bikeshare data is sourced from the official website of Divvy bike, facilitating the construction of a bikeshare trip network, which elucidates features indicative of the utilization of bike facilities within each census tract. These data sources give rise to two subsidiary research questions: (1) Can house density serve as a reflection of poverty levels within a neighborhood? (2) How does bikeshare facility usage relate to poverty levels?

To tackle these queries, a random forest regressor is trained utilizing the features derived from satellite imagery and the bikeshare network. The median household income, sourced from the American Community Survey report, is employed as an indicator of neighborhood poverty. Model performance is assessed, and an analysis of feature importance is conducted. Consequently, the research findings reveal significant insights into the relationship between urban infrastructure, poverty levels, and bikeshare facility usage.

Over the course of the study, approximately 10 thousand images were collected and processed, and a network was constructed based on bikeshare trips encompassing over 5 million samples. To tackle the challenges posed by these vast datasets, innovative strategies such as Spark distributed computing, AWS Lambda parallelization, and MPI parallelization were effectively deployed, resulting in accelerated computation times.

# Project Summary
The whole project is split into four parts: Satellite image collecting, image processing, network analysis and spark machine learning.

- **1_Image_Collecting** [satellite_image_collection.ipynb](https://github.com/macs30123-s23/final-project-urban_explorer/blob/main/1_Image_Collecting/satellite_image_collection.ipynb) and [lambda_function.py](https://github.com/macs30123-s23/final-project-urban_explorer/blob/main/1_Image_Collecting/lambda_function.py)
    
    This part sets step functions on AWS lambda which collects satellite images using MapBox API (https://docs.mapbox.com/api/maps/static-images/) and uploads the collected images to AWS S3 bucket. This study ramdomly samples 10 images (each image covers an area of 50m*50m) within each census tract and parallelly collects overall around 10k images. A mapping file is also generated which maps the image file name with the census tract id for the analysis in next step.

- **2_Satellite_Image_Processing** [mpi_area_extraction.py](https://github.com/macs30123-s23/final-project-urban_explorer/blob/main/2_Satellite_Image_Processing/mpi_area_extraction.py) and [mpi.sbatch](https://github.com/macs30123-s23/final-project-urban_explorer/blob/main/2_Satellite_Image_Processing/mpi.sbatch)

    This part parallelly processes the 7850 satellite image to extract feature of house density using MPI parallelization. This study uses 10 nodes, distributes images evenly, and gathers the processed results in rank 0.

- **3_Network_Analysis_GraphFrames** [network_analysis.ipynb](https://github.com/macs30123-s23/final-project-urban_explorer/blob/main/3_Network_Analysis_GraphFrames/network_analysis.ipynb)

    This part uses Spark’s GraphFrames package to conduct network analysis upon over 5 million bikeshare trip data. This study uses geo-join to map census tracts with the start and end points in the original data. Each node represents one census tract and each edge represents a trip between two tracts. This study extracts node statistics from the network including degrees, PageRank, clustering coefficients and links to high income area for the use in following machine learning. 

- **4_Spark_Machine_Learning** [SparkML.ipynb](https://github.com/macs30123-s23/final-project-urban_explorer/blob/main/4_Spark_Machine_Learning/sparkML.ipynb)

    This part uses the extracted features including node statistics and house density along with socio-economic features from census data to train a random forest regressor in Spark. The house density of the area is defined to be the average area of the images extracted from this tract. This study uses 5 fold cross validation to select the best number of trees to use in the random forest regressor and compares the performaces of random forest with only urban features (node statistics + house density), and full features (urban features + socio-economic features).

# Findings
Model Performance:

![image](https://github.com/macs30123-s23/final-project-urban_explorer/blob/main/Findings/model%20performance.png)
In the machine learning part, we trained two models with different selection of features. We put urban features along with demographic features in one model and put only urban features in the other model. Comparing two random forest models with different selection of features:
The model with demographic features has an r-square score of 0.79 and the model with urban features only has an r-square score of 0.66. There is significant explanatory power of these urban features.

Feature importance of two models:

![image](https://github.com/macs30123-s23/final-project-urban_explorer/blob/main/Findings/RF_importance_full.png)
![image](https://github.com/macs30123-s23/final-project-urban_explorer/blob/main/Findings/RF_importance_urban.png)
Among the socio-demographic features, the percentage of white residents and college education level emerged as significant contributors. Higher percentages of white residents and a higher level of college education are associated with lower poverty levels. These findings suggest that educational attainment and racial composition play key roles in shaping socioeconomic conditions within neighborhoods.

Additionally, among the urban features we extracted, the area reflecting house density stands out as the most important feature related to poverty levels. This indicates that areas with lower house density, which may be associated with higher greening rates and larger open spaces, tend to have lower poverty levels. This finding aligns with the notion that neighborhoods with more spacious and well-maintained environments often correspond to higher-income areas.

In addition to house density, our analysis reveals that other factors contribute significantly to predicting poverty levels. Two notable factors are the degree extracted from the network and the links to high-income areas. The degree of a census tract represents its level of connectivity and interaction with other tracts in the network. Tracts with higher degrees indicate a greater number of social and economic exchanges, which can influence poverty levels. This finding suggests that areas with more connections may have increased opportunities for resources and support, potentially leading to lower poverty rates. Furthermore, the links to high-income areas play a significant role in predicting poverty levels. This factor measures the number of trips originating from high-income areas to the census tract under consideration. Tracts with stronger connections to high-income areas indicate a higher likelihood of being in a lower poverty level. This finding suggests that proximity and accessibility to affluent regions may positively impact socioeconomic conditions in a given neighborhood.

# References
- Hall, Ola, et al. “A Review of Machine Learning and Satellite Imagery for Poverty Prediction: Implications for Development Research and Applications.” JOURNAL OF INTERNATIONAL DEVELOPMENT, Feb. 2023. EBSCOhost, https://doi-org.proxy.uchicago.edu/10.1002/jid.3751.

- Guérin, E., Oechslin, K., Wolf, C., & Martinez, B. (2021). Satellite Image Semantic Segmentation.

- Singh, N. J., & Nongmeikapam, K. (2023). Semantic Segmentation of Satellite Images Using Deep-Unet. Arabian Journal for Science & Engineering (Springer Science & Business Media B.V. ), 48(2), 1193–1205. https://doi-org.proxy.uchicago.edu/10.1007/s13369-022-06734-4

- Gardiner, O., & Dong, X. (2021). Mobility Networks for Predicting Gentrification (Vol. 944). Springer Science and Business Media Deutschland GmbH. https://doi.org/10.1007/978-3-030-65351-4_15

- Oates, G. R., Hamby, B. W., Bae, S., Norena, M. C., Hart, H. O., & Fouad, M. N. (2017). Bikeshare Use in Urban Communities : Individual and Neighborhood Factors. Ethnicity & Disease, 27, 303–312.

- Zixi, H. (2021). Poverty Prediction Through Machine Learning. 2021 2nd International Conference on E-Commerce and Internet Technology (ECIT), E-Commerce and Internet Technology (ECIT), 2021 2nd International Conference on, ECIT, 314–324. https://doi-org.proxy.uchicago.edu/10.1109/ECIT52743.2021.00073
