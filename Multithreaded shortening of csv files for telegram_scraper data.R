#install.packages("fwrite")
library("naniar")
#library("fwrite")
library("plyr")  
library("dplyr") 
library("readr")  
library("tibble")
library("data.table")

#get a list of all csv files
filelist = list.files(path = "C:\\Telegram Daten final",     # Identify all csv files in folder
                      pattern = ".csv", full.names = TRUE)

#_______________________________PARALLEL COMPUTING_____________________________#
# Setting up a cluster in a single computer requires first to find out how many cores we want to use from the ones we have available. It is recommended to leave one free core for other tasks.
#automatic install of packages if they are not installed already
list.of.packages <- c(
  "foreach",
  "doParallel",
  "ranger",
  "palmerpenguins",
  "tidyverse",
  "kableExtra"
)

new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]

if(length(new.packages) > 0){
  install.packages(new.packages, dep=TRUE)
}

#loading packages
for(package.i in list.of.packages){
  suppressPackageStartupMessages(
    library(
      package.i, 
      character.only = TRUE
    )
  )
}
#okaaaay lets go
parallel::detectCores()
n.cores <- parallel::detectCores() - 1

#create the cluster
my.cluster <- parallel::makeCluster(
  n.cores, 
  type = "PSOCK"
)

#check cluster definition (optional)
print(my.cluster)

#register it to be used by %dopar%
doParallel::registerDoParallel(cl = my.cluster)

#check if it is registered (optional)
foreach::getDoParRegistered()

#how many workers are available? (optional)
foreach::getDoParWorkers()
#save na-strings for later
na_strings <- c("NA", "N A", "N / A", "N/A", "N/ A", "Not Available", "NOt available", "false", "FALSE")

start_time <- Sys.time()
x = foreach(
  i = filelist, .packages= c('dplyr', 'naniar', 'data.table','plyr', 'tibble', 'readr') 
) %dopar% {
  nms <- c("id", "message", "date", "peer_id_channel_id", "fwd_from_from_id_channel_id", "post_author", "fwd_from_date", "fwd_from_channel_post", "entities_url", "media_webpage_url", "views", "forwards", "replies", "replies_replies", "replies__")   # Vector of columns you want in this data.frame
  
  df <- as.data.frame(fread(i, header=TRUE, encoding = "UTF-8"))
  Missing <- setdiff(nms, names(df))  # Find names of missing columns
  df[Missing] <- NA                    # Add them, filled with 'NA's
  df <- df[nms]  
  # now pick only those columns needed
  df = select(df, c("id", "message", "date", "peer_id_channel_id", "fwd_from_from_id_channel_id", "post_author", "fwd_from_date", "fwd_from_channel_post", "entities_url", "media_webpage_url", "views", "forwards", "replies", "replies_replies", "replies__"))
  df %>%
    replace_with_na_all(condition = ~.x %in% c("NA", "N A", "N / A", "N/A", "N/ A", "Not Available", "NOt available", "false", "FALSE")) 
  filename = paste0(i, ".csv")
  df[] <- lapply(df, as.character) #seemed easiest to make evth. as character to preserve data (had some issues here before)
  write.csv(df, filename)
}
end_time <- Sys.time()
parallel::stopCluster(cl = my.cluster)
#use below if loop doesnt work anymore suddenly. Start everything again from registering on
#unregister_dopar <- function() {
# env <- foreach:::.foreachGlobals
# rm(list=ls(name=env), pos=env)
#}
#perhaps it's better for you to save not as csv in the end of the loop but as rds with "saveRDS(df, filename)" in line 84
#you`d have to adjust to "filename = paste0(i, ".rds")" then in line 82 as well.
