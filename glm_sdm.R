# Species distribution modeling for Lycaena arota and host plants
# Rachel Laura
# rlaura@arizona.edu
# 2023-08-16

library(terra)
library(geodata)
library(predicts)

# Download bioclimatic variable data
bioclim_data <- worldclim_global(var = "bio", # download all 19 bioclimatic variables
                                 res = 2.5, # 2.5 minutes of a degree resolution
                                 path = "data/") # location to download to

# Read in observations for Lycaena arota (update to add all species eventually)
obs_data <- read.csv(file = "data/gbif/downloaded/lycaena_arota-gbif.csv")

# Check the data to make sure it loaded correctly
summary(obs_data)

# Remove NAs (if needed)
# obs_data <- obs_data[!is.na(obs_data$latitude),]

# Determine geographic extent of our data
max_lat <- ceiling(max(obs_data$latitude))
min_lat <- floor(min(obs_data$latitude))
max_lon <- ceiling(max(obs_data$longitude))
min_lon <- floor(min(obs_data$longitude))

# Store boundaries in a single extent object
geographic_extent <- ext(x = c(min_lon, max_lon, min_lat, max_lat))

# Download data with geodata's world function to use for base map
world_map <- world(resolution = 3, path = "data/")

# Crop the map to our area of interest
my_map <- crop(x = world_map, y = geographic_extent)

# Plot the base map
plot(my_map,
     axes = TRUE,
     col = "grey95")

# Add the points for individual observations
points(x = obs_data$longitude,
       y = obs_data$latitude,
       col = "olivedrab",
       pch = 20,
       cex = 0.75)

# Make an extent that is 25% larger
sample_extent <- geographic_extent * 1.25

# Crop bioclim data to desired extent
bioclim_data <- crop(x = bioclim_data, y = sample_extent)

# Plot the first of the bioclim variables to check on cropping
plot(bioclim_data[[1]])

# Create a set of background points at random, then add these to our data

# Set the seed for the random-number generator to ensure results are similar
set.seed(20230817)

# Randomly sample points (same number as our observed points)
background <- spatSample(x = bioclim_data,
                         size = 1000,     # generate 1,000 pseduo-absence points
                         values = FALSE,  # don't need values
                         xy = TRUE)       # just need coordinates

# Look at first few rows of background
head(background)

# Plot these points to see how the random sampling looks
# Plot the base map
plot(my_map,
     axes = TRUE,
     col = "grey95")

# Add background points
points(background,
       col = "grey30",
       pch = 1,
       cex = 0.75)

# Add points for observations
points(x = obs_data$longitude,
       y = obs_data$latitude,
       col = "olivedrab",
       pch = 20,
       cex = 0.75)

# FIGURE OUT HOW TO CROP THE OCEAN OUT

# Create a single data frame that contains both presence and pseudo-absence data,
# then add the climate data for each point. 

# Pull out coordinate columns, x (long) first, then y (lat) from data
presence <- obs_data[, c("longitude", "latitude")]

# Add column indicating presence with a 1
presence$pa <- 1

# Convert background data to a data frame
absence <- as.data.frame(background)

# Update column names so they match with presence points
colnames(absence) <- c("longitude", "latitude")

# Add column indicating absence with a 0
absence$pa <- 0

# Join data into a single data frame
all_points <- rbind(presence, absence)

# Reality check on data
head(all_points)

# Add in the climate data
# We are now ready to add climate data to the coordinate data sets. We will use
# the extract() function, which takes geographic coordinates and raster data as
# inputs, and pulls out values in the raster data for each of the coordinates.
bioclim_extract <- extract(x = bioclim_data,
                           y = all_points[, c("longitude", "latitude")],
                           ID = FALSE) # No need for an ID column

# The process of extracting data results in a data frame with the climate data, 
# but that data frame doesn't have the coordinate information and, more importantly
# doesn't indicate which rows are presence points and which are psuedo-absence
# points. So we need to join these extracted data back with our all_points
# data frame. After we do this, we do not need lat long coords anymore, so we
# can drop those two columns (at least for building the model.

# Add the points and climate datasets together
points_climate <- cbind(all_points, bioclim_extract)

# Identify columns that are lat and long
drop_cols <- which(colnames(points_climate) %in% c("longitude", "latitude"))
drop_cols

# Remove the geographic coordinates from the data frame
points_climate <- points_climate[, -drop_cols]

# Split dataset into training and testing
# Now that we have climate data for our presence and absence points, we need to
# take one more step. We are doing to build our model using only part of our data.
# We will use the "set aside" data to evaluate model performance afterward. This
# is known as separating our data into a training set and a testing set. We are 
# going to reserve 20% of our data for testing, so we will use the folds() function
# from the predicts package to evenly assign each point to a random group. We 
# will use the pa column to tell R that our data has these two sub-groups.

# Create vector indicating fold
fold <- folds(x = points_climate,
              k = 5, 
              by = points_climate$pa)

# We now can use the fold vector to split data into a training set and a testing 
# set. Values in the fold vector are integers 1-5, each evenly sampled. We can 
# see this with the table() function, which counts how many times each of the 
# fold values occurs

table(fold) # check that the folds are evenly split amongst the data

# Fold 1 will be testing data, 2-5 will be training data
testing <- points_climate[fold == 1,]
training <- points_climate[fold !=1, ]

# Build a model using training data (Generalized Linear Model)
glm_model <- glm(pa ~., data = training, family = binomial())

# pa ~ . This is the forumala that we are analyzing, that is, we are asking R
# to predict the values in the pa column based on the values in ALL the remaining
# columns. Instead of listing the names of all the bioclim variables, we can just
# use dot(.) to mean "all the columns except the column to the left of the tilda

# data = training tells R to use only the data stored in the training data frame
# to build the model

# family = binomail() Because pa can only be 0 or 1, we have to indicate to R

# Now that we have built our model, we can use it to predict the habitat suitability
# across the entire map. We do this with the predict() function, passing the data
# to feed into the model (bioclim_data), the stored model itself (glm_model), and 
# finally what values we want as output (type = "response"). This last argument
# will return the predicted probabilities from our model. 

# Get predicted values from the model
glm_predict <- predict(bioclim_data, glm_model, type = "response")

# Print predicted values
plot(glm_predict)

# We now take that model, and evaluate it using the observation data and the 
# psuedo-absence data we reserved for model testing. We then use this test to 
# establish a cutoff of occurrence probability to determine the boundaries of 
# the saguaro range

# Use testing data for model evaluation
glm_eval <- pa_evaluate(p = testing[testing$pa == 1, ],
                        a = testing[testing$pa == 0, ],
                        model = glm_model)

# We pass 3 pieces of info to the function above: 
# 1) "p = ..." p stands for presence data, so we pass all the rows in the testing
# data that correspond to a location where there was a saguaro present
# 2) "a = ..." a stands for absence data, we pass all pseudo-absence rows in our
# dataset 
# 3) model = glm_model - This is the model object we are evaluating. Think of it
# as the calculator that takes the bioclim data and input and provides probabilities 
# as output

# With the pa_evaluate function, we pass data that we "know" what the right answer
# should be for these probability calculation. That is, the model should predict
# values close to 1 for those rows that we pass to the p argument and should predict
# values close to 0 for those rows that we pass the a agrument. We use this info 
# on model performance to determine the probability value to use as a cutoff to
# saying whether a particular location is suitable or unsuitable for saguaros.

# Determine minimum threshold for "presence"
glm_threshold <- glm_eval@thresholds$max_spec_sens

# The thresholds element of glm_eval offers a number of means of determining the 
# threshold cutoff. Here we chose max_spec_sens, which sets "the threshold at which
# the sum of the sensitivity (true positive rate) and specificity (true negative rate)
# is highest."

# Now we use the threshold to paint a map with sites predcicted to be suitable.
# Plot base map
plot(my_map,
     axes = "TRUE",
     col = "grey95")

# Only plot areas where the probability of occurrence is > the threshold
plot(glm_predict > glm_threshold,
     add = TRUE,
     legend = FALSE,
     col = c(NA, "olivedrab"))

# And add those observations
points(x = obs_data$longitude,
       y = obs_data$latitude,
       col = "black",
       pch = "+",
       cex = 0.75)

# Redraw those country borders
plot(my_map, 
     add = TRUE,
     border = "grey5")


