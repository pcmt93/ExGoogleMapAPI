###############################################################

# Created February 11, 2016 by Kate Pennington (kate.pennington@berkeley.edu)
# Adapted July 16, 2018 by Matt Pecenco and Carlos Schmidt-Padilla
# Readapted Sep 20, 2018 by Paulo Matos for El Salvador addresses
# and in 11/19 for Peru case 

##############################################################################

rm(list = ls())

# # Prev installing packages only if it is neccesary 
# clist <- c("magrittr", "foreign", "placement", 
#            "readstata13", "readxl")
# 
# for (i in clist) {
#   
#   install.packages(i)
# }

beginning <- Sys.time()

library(foreign)
library(magrittr)
library(placement)
library(readstata13)
library(readxl)

# Working directory 

root <- paste("/Users/paulomatos/Dropbox", sep = "/")
root2 <- paste("/Proyecto Matching Contribution 2017-2018", sep = "/")
rpeF <- paste0(root, root2, "/04_ResearchDesign/03 Randomization/PROYECTO/data")

# put API in quotations like example below
key <- "" # You need to get an API google key


###############################################################################

# A. Prep bc data

###############################################################################

round <- 1
id_var <- "codigoempresa"

outputSuffix <- paste0("/190115--geo-mc-geo", ".csv") ## for now keep it, 
# then check if it is necessary 

#full set of non-geocoded

rpe <- read.csv(paste0(rpeF,"/150119-direccion_clean-sa.csv"))

dim(rpe)
rpe <- rpe[!duplicated(rpe[, c(id_var)]), ]


#previously geocoded by google / al parecer tienes que correr el programa cada 1000 veces 
# podria readecuarlo para que sea mas directo 


if (file.exists(paste0(rpeF, outputSuffix))) {
  previouslyGeocoded <- read.csv(
    paste0(rpeF, outputSuffix))
  rpe <- merge(rpe, previouslyGeocoded, all = TRUE)
} else {
  rpe$google_status <- NA
}

#keep non-geocoded
rpeAddresses <- rpe[is.na(rpe$google_status),] #as of yet, not attempted to be geocoded
rpeAddresses <- rpeAddresses[, c(id_var, "dir1_completa")]
rpeAddresses <- rpeAddresses[!duplicated(rpeAddresses), ]
#rpeAddresses <- rpeAddresses[50001:nrow(rpeAddresses), ]
#rpeAddresses <- rpeAddresses[100001:nrow(rpeAddresses), ]
#rpeAddresses <- rpeAddresses[150001:nrow(rpeAddresses), ]
#rpeAddresses <- rpeAddresses[200001:nrow(rpeAddresses), ]
#rpeAddresses <- rpeAddresses[nrow(rpeAddresses):1, ]

###############################################################

# B. Geocode rpes' addresses

###############################################################

#if you want to sample a set
sample <- FALSE
if (sample == TRUE) {
  set.seed(1)
  randomSample <- sample(x = rpeAddresses[[id_var]], size = 1000)
  rpeAddresses <- rpeAddresses[rpeAddresses[[id_var]] %in% randomSample, ]
}

# 
n <- 100000
if (nrow(rpeAddresses) < n) {
  iterations <- nrow(rpeAddresses)
} else {
  iterations <- n
}
output <- data.frame(matrix(NA, ncol = 10, nrow = iterations)) 
names(output) <- c(id_var, "dir1_completa","lat","lon", "type", 
                   "formatted_addr", "status", "error_message", 
                   "locations", "url")
for(i in 1:iterations) {
  geoloc = geocode_url(rpeAddresses$dir1_completa[i],
                       privkey= key, clean = TRUE)
  entry=cbind(rpeAddresses[i,],geoloc)
  if (entry$status == "OVER_QUERY_LIMIT") {
    Sys.sleep(4)
    geoloc = geocode_url(rpeAddresses$dir1_completa[i],
                         privkey= key, clean = TRUE)
    entry=cbind(rpeAddresses[i,],geoloc)
    if (entry$status == "OVER_QUERY_LIMIT") {
      paste("still being throttled")
      break
    }  
  }
  output[i, ] <- entry
  print(i)
}
names(output)[names(output) == "status"] <- "google_status"

#remove any possible NA observations (shouldn't be any though)
output <- output[!is.na(output[[id_var]]), ]
dim(output)

###############################################################

# C. Output geocoded.csv

###############################################################

if (file.exists(paste0(rpeF, outputSuffix))) {
  previouslyGeocoded <- read.csv(
    paste0(rpeF, outputSuffix))
  final <- rbind(previouslyGeocoded, output)
} else {
  final <- output
}

write.csv(final, file = paste0(rpeF, outputSuffix),
          row.names = FALSE)
print(Sys.time())
print(beginning)


