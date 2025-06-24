FROM amancevice/pandas:1.4.3

RUN pip install argparse \
                synapseclient \
                git+https://github.com/larssono/bridgeclient.git \
                git+https://github.com/Sage-Bionetworks/synapsebridgehelpers
RUN git clone -b master https://github.com/Sage-Bionetworks/synapsebridgehelpers.git /root/synapsebridgehelpers
