FROM amancevice/pandas:1.5.3

RUN pip install synapseclient==4.8.0 \
                boto3~=1.38.0 \
                git+https://github.com/larssono/bridgeclient.git@b90eb4c \
                git+https://github.com/Sage-Bionetworks/synapsebridgehelpers@caf502d

RUN git clone -b master https://github.com/Sage-Bionetworks/synapsebridgehelpers.git /root/synapsebridgehelpers
