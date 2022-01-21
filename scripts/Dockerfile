FROM rocker/tidyverse:4.1.0

RUN apt install libffi-dev
RUN Rscript -e 'install.packages(c("devtools", "optparse"))'
RUN Rscript -e 'devtools::install_github("Sage-Bionetworks/synapser", ref="0.10.89")'
RUN Rscript -e 'devtools::install_github("philerooski/bridgeclient", ref="857fd75")'
ENV dummy=$(date)
RUN git clone https://github.com/Sage-Bionetworks/synapsebridgehelpers.git /root/synapsebridgehelpers
