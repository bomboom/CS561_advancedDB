library(rmr2)

map <- function(k,lines) {
  a = 1:250000
  line <- strsplit(lines, '\\n')
  b = rapply(line, function(row){
    cc = unlist(strsplit(row, ','))
    cc = cc[seq(4,length(a),4)]
  })
  keyval(b,"1")
}

reduce <- function(word, counts) {
  keyval(word, toString(sum(as.numeric(counts))))
}

wordcount <- function (input, output=NULL) {
  mapreduce(input=input, output=output, input.format="text", map=map, reduce=reduce)
}


out <- wordcount('/user/hadoop/input/Customers.csv')

## Fetch results from HDFS
results <- from.dfs(out)

# (2)
x <- results$key
y <- as.integer(results$val)
barplot(y, main="Country Code - Customer Count", 
        xlab="C_Code", ylab="CustCount", names.arg=x)

# (3)
x <- x[order(y, decreasing=T)]
z <- sort(y, decreasing=T)
barplot(z, main="Country Code - Customer Count", 
        xlab="C_Code", ylab="CustCount", names.arg=x, col="blue")
