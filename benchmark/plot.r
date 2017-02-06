v = read.table(file("results.log"))
t <- data.frame(readers=v[,1], writers=v[,2], distribution=v[,3], variant=v[,4], wopss=v[,5], ropss=v[,7])
t$wopss = t$writers * t$wopss / 1000.0
t$ropss = t$readers * t$ropss / 1000.0
t

library(ggplot2)

t$writers = as.factor(t$writers)
t$readers = as.numeric(t$readers)
p <- ggplot(data=t, aes(x=readers, y=ropss, color=variant))
p <- p + ylim(c(0, 5000))
#p <- p + scale_y_log10()
p <- p + xlim(c(0, NA))
p <- p + facet_grid(distribution ~ writers, labeller = labeller(writers = label_both))
p <- p + geom_point(size = 2) + geom_line()
p <- p + xlab("readers") + ylab("k reads/s") + ggtitle("Total reads/s with increasing # of readers")
ggsave('read-throughput.png',plot=p,width=10,height=6)


#t$writers = as.numeric(t$writers)
#t$readers = as.factor(t$readers)
p <- ggplot(data=t, aes(x=readers, y=wopss, color=variant))
p <- p + ylim(c(0, 5000))
#p <- p + scale_y_log10()
p <- p + xlim(c(0, NA))
p <- p + facet_grid(distribution ~ writers, labeller = labeller(writers = label_both))
p <- p + geom_point(size = 2) + geom_line()
p <- p + xlab("readers") + ylab("k writes/s") + ggtitle("Total writes/s with increasing # of readers")
ggsave('write-throughput.png',plot=p,width=10,height=6)
