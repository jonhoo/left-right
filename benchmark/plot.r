v = read.table(file("results.log"))
t <- data.frame(readers=v[,1], writers=v[,2], distribution=v[,3], variant=v[,4], opss=v[,5], op=v[,7])

library(plyr)
t$writers = as.factor(t$writers)
t$readers = as.numeric(t$readers)
r = t[t$op == "read",]
r <- ddply(r, c("readers", "writers", "distribution", "variant", "op"), summarise, opss = sum(opss))
w = t[t$op == "write",]
w <- ddply(w, c("readers", "writers", "distribution", "variant", "op"), summarise, opss = sum(opss))

library(ggplot2)

r$opss = r$opss / 1000000.0
p <- ggplot(data=r, aes(x=readers, y=opss, color=variant))
#p <- p + ylim(c(0, 2500))
p <- p + xlim(c(0, NA))
p <- p + facet_grid(distribution ~ writers, labeller = labeller(writers = label_both))
p <- p + geom_point(size = .4, alpha = .1)
p <- p + geom_line(size = .5)
#p <- p + stat_smooth(size = .5, se = FALSE)
p <- p + xlab("readers") + ylab("M reads/s") + ggtitle("Total reads/s with increasing # of readers")
ggsave('read-throughput.png',plot=p,width=10,height=6)


w$opss = w$opss / 1000000.0
p <- ggplot(data=w, aes(x=readers, y=opss, color=variant))
#p <- p + scale_y_log10(lim=c(1, NA))#5000))
p <- p + facet_grid(distribution ~ writers, labeller = labeller(writers = label_both))
p <- p + geom_point(size = 1, alpha = .2)
p <- p + geom_line(size = .5)
#p <- p + stat_smooth(size = .5, se = FALSE)
#p <- p + coord_cartesian(ylim=c(0,250))
p <- p + xlim(c(0, NA))
p <- p + xlab("readers") + ylab("M writes/s") + ggtitle("Total writes/s with increasing # of readers")
ggsave('write-throughput.png',plot=p,width=10,height=6)
