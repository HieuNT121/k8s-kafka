# Phase 6: Kafka Networking và Client Connectivity trên Kubernetes

Phase này giải thích cách một client trong Kubernetes kết nối tới Kafka thông qua Service, DNS và Kafka listener. Tài liệu sử dụng chính các output thực tế của project:

```text
Service: kafka-cluster-kafka-bootstrap
ClusterIP: 10.96.225.53
Kafka client port: 9092
Broker endpoints: 10.244.0.83, 10.244.0.84, 10.244.0.85
Client Pod: kafka-client-test
```

Mục tiêu là hiểu chính xác ba lớp khác nhau:

```text
DNS resolution -> TCP connectivity -> Kafka protocol connectivity
```

`nslookup` thành công chỉ chứng minh DNS. `nc` báo `open` chỉ chứng minh TCP. Kafka client còn phải nhận metadata và kết nối được tới các địa chỉ broker mà Kafka advertise.

## 1. Kết quả cần đạt

Sau hướng dẫn này, bạn có thể:

- Đọc `kubectl describe svc` của bootstrap Service.
- Giải thích `ClusterIP`, selector, target port và endpoints.
- Hiểu DNS short name và DNS đầy đủ trong Kubernetes.
- Hiểu vì sao BusyBox `nslookup` có thể in `NXDOMAIN` nhưng vẫn resolve được Service.
- Kiểm tra DNS và TCP từ một client Pod.
- Phân biệt `localhost:9092` với Kafka Service DNS.
- Hiểu giới hạn của kiểm tra `nc` đối với Kafka.
- Phân biệt internal listener và external listener.
- Kiểm tra Kafka metadata khi client vẫn lỗi sau khi TCP đã thông.

## Network foundations: nền tảng network cần biết trước

Trước khi đọc Kubernetes YAML, hãy tách một kết nối thành các câu hỏi nhỏ:

```text
1. Client là ai và đang chạy ở đâu?
2. Client tìm địa chỉ server bằng cách nào?
3. Client mở kết nối tới IP và port nào?
4. Thành phần nào chuyển tiếp traffic tới backend?
5. Ứng dụng nói giao thức gì sau khi TCP đã mở?
```

Trong project này:

```text
Client       = kafka-client-test Pod
Name lookup  = Kubernetes DNS / CoreDNS
Virtual IP   = Service ClusterIP 10.96.225.53
Port         = TCP 9092
Backend      = ba Kafka node Pod
Protocol     = Kafka protocol
```

### IP address trong Kubernetes

IP là địa chỉ để định tuyến packet tới một network interface. Kubernetes có nhiều loại IP:

| Địa chỉ | Ví dụ | Ý nghĩa |
| --- | --- | --- |
| Pod IP | `10.244.0.87` | Địa chỉ network của một Pod cụ thể |
| Broker Pod IP | `10.244.0.83` | Địa chỉ Kafka node hiện tại, có thể đổi |
| Service ClusterIP | `10.96.225.53` | Virtual IP ổn định của Service |
| DNS server IP | `10.96.0.10` | Địa chỉ CoreDNS Service |

Trong output của bạn:

```text
kafka-client-test Pod       -> 10.244.0.87
kafka-cluster-kafka-bootstrap -> 10.96.225.53
Kafka broker Pods           -> 10.244.0.83/.84/.85
CoreDNS                     -> 10.96.0.10
```

Client nên dùng Service DNS/ClusterIP, không dùng trực tiếp broker Pod IP vì Pod IP không ổn định.

### Port, TCP và UDP

Một IP có thể chạy nhiều network service. Port xác định process hoặc service nhận traffic:

```text
IP address + TCP port = network endpoint
10.96.225.53:9092     = Kafka client endpoint
```

Các port trong project:

```text
53   = DNS query tới CoreDNS
9091 = Kafka broker/internal replication listener
9092 = Kafka client listener
```

TCP là connection-oriented protocol. `nc -zv` kiểm tra việc mở TCP socket, nhưng không gửi Kafka metadata request. DNS thường dùng UDP port 53 cho query nhỏ; Kafka listener dùng TCP.

### DNS là gì?

DNS chỉ ánh xạ tên sang địa chỉ:

```text
kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local
        |
        v
10.96.225.53
```

Sau khi có IP, client mới mở TCP connection tới port `9092`. DNS thành công chưa có nghĩa port mở; port mở chưa có nghĩa Kafka protocol hoạt động.

### Network namespace và `localhost`

Mỗi Pod có network namespace riêng. Container trong cùng Pod thường dùng chung network namespace, nhưng Pod khác thì không:

```text
Kafka Pod 0:
  localhost -> Kafka Pod 0

kafka-client-test Pod:
  localhost -> kafka-client-test Pod
```

Vì vậy `localhost:9092` trong client Pod không trỏ tới Kafka Pod. Client phải dùng:

```text
kafka-cluster-kafka-bootstrap:9092
```

### HTTP khác Kafka protocol

Kubernetes Service chỉ chuyển tiếp network traffic; nó không hiểu nội dung Kafka message. Kafka broker mới là thành phần hiểu topic, partition, metadata, producer và consumer.

```text
Service: biết IP, port, endpoint và routing
Kafka:   hiểu Kafka metadata và Kafka records
```

Vì vậy `nc` có thể báo `open` dù Kafka client vẫn lỗi metadata.

## Kubernetes networking: Pod, Service và EndpointSlice

### Pod network

Trong Kubernetes:

- Mỗi Pod nhận một IP riêng.
- Pod có thể giao tiếp với Pod khác qua cluster network.
- Pod IP có thể thay đổi khi Pod bị recreate.
- Container trong cùng Pod chia sẻ network namespace.

Minikube hiện có một node nhưng nhiều Pod IP:

```text
minikube node
  +-- Kafka Pod 0: 10.244.0.83
  +-- Kafka Pod 1: 10.244.0.84
  +-- Kafka Pod 2: 10.244.0.85
  +-- Client Pod: 10.244.0.87
```

### Service là địa chỉ ổn định trước backend động

```text
Client
  |
  v
Service DNS / ClusterIP ổn định
  |
  +-- Kafka Pod A hiện tại
  +-- Kafka Pod B hiện tại
  +-- Kafka Pod C hiện tại
```

Service không phải một Kafka process. `ClusterIP` là virtual IP được Kubernetes networking xử lý để đưa traffic tới endpoint phù hợp.

### Selector và EndpointSlice

Service dùng selector để tìm Pod:

```text
Service selector -> Pod labels -> EndpointSlice -> Service routing
```

Nếu Pod chưa Ready hoặc label không match, Pod có thể không xuất hiện trong endpoint ready. Service vẫn có thể có ClusterIP nhưng request sẽ timeout hoặc bị refused.

### `port` và `targetPort`

```yaml
ports:
  - port: 9092
    targetPort: 9092
```

- `port`: port mà client gọi trên Service.
- `targetPort`: port backend Pod nhận traffic.

Trong output, cả hai được hiển thị bằng tên `tcp-clients`. Client gọi `Service:9092`; Service chuyển traffic tới broker endpoint port `9092`.

## Luồng packet từ client tới Kafka

Khi chạy `nc -zv kafka-cluster-kafka-bootstrap 9092`:

```text
1. Resolver đọc /etc/resolv.conf
2. Resolver hỏi CoreDNS 10.96.0.10:53
3. CoreDNS trả ClusterIP 10.96.225.53
4. Client mở TCP tới 10.96.225.53:9092
5. Kubernetes Service chọn endpoint broker ready
6. Traffic tới một broker Pod:9092
```

Khi chạy Kafka CLI, thêm các bước:

```text
7. Kafka client gửi metadata request
8. Broker trả topic/partition/leader metadata
9. Client kết nối broker leader tương ứng
10. Producer hoặc consumer thực hiện Kafka operation
```

Đây là lý do phải kiểm tra theo tầng: DNS -> TCP -> Kafka protocol.

## 2. Sơ đồ kết nối hiện tại

```text
kafka-client-test Pod
        |
        | DNS: kafka-cluster-kafka-bootstrap
        v
Kubernetes DNS / CoreDNS
        |
        | Service ClusterIP: 10.96.225.53
        v
kafka-cluster-kafka-bootstrap Service
        |
        | selector chọn Kafka broker Pods
        +----------------+----------------+
        v                v                v
Kafka node 0        Kafka node 1       Kafka node 2
10.244.0.83:9092    10.244.0.84:9092   10.244.0.85:9092
```

Client không kết nối trực tiếp tới một Pod IP cố định. Client dùng Service làm bootstrap entry point; Kubernetes Service phân phối request tới các broker endpoint phù hợp.

## 3. Đọc output `kubectl describe svc`

Chạy:

```bash
kubectl describe svc kafka-cluster-kafka-bootstrap -n kafka
```

### 3.1. `Name` và `Namespace`

```text
Name:      kafka-cluster-kafka-bootstrap
Namespace: kafka
```

Service có tên `kafka-cluster-kafka-bootstrap` trong namespace `kafka`. Tên này do Strimzi tạo dựa trên Kafka cluster name `kafka-cluster`.

Trong cùng namespace, client có thể dùng short name:

```text
kafka-cluster-kafka-bootstrap:9092
```

Từ namespace khác, nên dùng:

```text
kafka-cluster-kafka-bootstrap.kafka:9092
```

Hoặc full DNS:

```text
kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
```

### 3.2. Labels

Output có các label như:

```text
app.kubernetes.io/managed-by=strimzi-cluster-operator
strimzi.io/cluster=kafka-cluster
strimzi.io/component-type=kafka
strimzi.io/discovery=true
```

Các label cho biết Service do Strimzi quản lý và thuộc Kafka cluster nào. Không nên sửa hoặc xóa thủ công Service do Operator tạo; thay đổi nên đi qua Kafka CR và listener configuration.

### 3.3. Annotation discovery

```text
strimzi.io/discovery:
  [ {
    "port" : 9092,
    "tls" : false,
    "protocol" : "kafka",
    "auth" : "none"
  } ]
```

Annotation này mô tả listener cho client discovery:

- `port: 9092`: port Kafka client.
- `tls: false`: listener hiện không mã hóa TLS.
- `protocol: kafka`: đây là Kafka protocol, không phải HTTP.
- `auth: none`: listener hiện không yêu cầu authentication.

Đây là lab configuration. Không nên dùng `tls: false` và `auth: none` cho môi trường production nếu Kafka chứa dữ liệu nhạy cảm.

### 3.4. Selector

```text
Selector:
  strimzi.io/broker-role=true,
  strimzi.io/cluster=kafka-cluster,
  strimzi.io/kind=Kafka,
  strimzi.io/name=kafka-cluster-kafka
```

Selector là điều kiện Service dùng để chọn Pod backend. Chỉ Pod có đủ label phù hợp mới trở thành endpoint của Service.

Luồng:

```text
Service selector -> Pod labels -> EndpointSlice -> Service routing
```

Nếu selector không match Pod nào, Service vẫn có thể có ClusterIP nhưng không có endpoint hoạt động.

### 3.5. `Type: ClusterIP`

```text
Type: ClusterIP
IP:   10.96.225.53
```

`ClusterIP` là địa chỉ ảo chỉ dùng bên trong Kubernetes cluster. Nó không tự expose Kafka ra Mac hoặc Internet.

Vì client Pod `kafka-client-test` cũng chạy trong cluster, Pod này có thể truy cập ClusterIP.

Kiểm tra Service:

```bash
kubectl get svc kafka-cluster-kafka-bootstrap -n kafka -o wide
```

### 3.6. Hai port trong output

```text
Port:       tcp-replication  9091/TCP
TargetPort: tcp-replication/TCP

Port:       tcp-clients      9092/TCP
TargetPort: tcp-clients/TCP
```

Port `9091` phục vụ communication/replication nội bộ giữa Kafka broker. Client application cần dùng port `9092` của listener `plain` hiện tại.

Không dùng `9091` cho producer/consumer thông thường.

### 3.7. Endpoints

```text
Endpoints:
  10.244.0.84:9092,
  10.244.0.83:9092,
  10.244.0.85:9092
```

Đây là các địa chỉ Pod IP hiện đang được Service chọn cho port client `9092`. Các IP này có thể thay đổi khi Pod được recreate, nên application không nên hard-code chúng.

Kiểm tra bằng EndpointSlice, API hiện đại hơn resource `Endpoints`:

```bash
kubectl get endpointslice -n kafka \
  -l kubernetes.io/service-name=kafka-cluster-kafka-bootstrap
```

Hoặc xem YAML:

```bash
kubectl get endpointslice -n kafka \
  -l kubernetes.io/service-name=kafka-cluster-kafka-bootstrap \
  -o yaml
```

Nếu endpoint list trống, kiểm tra Pod labels, readiness và Service selector.

## 4. Kubernetes DNS hoạt động thế nào?

Kubernetes thường dùng CoreDNS. DNS Service của cluster thường nằm ở địa chỉ như:

```text
10.96.0.10
```

Một Pod trong namespace `kafka` có thể resolve Service short name:

```text
kafka-cluster-kafka-bootstrap
```

DNS đầy đủ theo cấu trúc:

```text
<service>.<namespace>.svc.<cluster-domain>
```

Với project này:

```text
kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local
```

Các dạng thường dùng:

```text
kafka-cluster-kafka-bootstrap
kafka-cluster-kafka-bootstrap.kafka
kafka-cluster-kafka-bootstrap.kafka.svc
kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local
```

Short name hoạt động nhờ search domains trong `/etc/resolv.conf` của Pod:

```bash
kubectl exec kafka-client-test -n kafka -- cat /etc/resolv.conf
```

Output thường có dạng:

```text
search kafka.svc.cluster.local svc.cluster.local cluster.local
nameserver 10.96.0.10
options ndots:5
```

Search domain cho phép resolver thử thêm suffix `kafka.svc.cluster.local` khi ứng dụng chỉ cung cấp short name.

## 5. Giải thích output `nslookup`

Lệnh đã chạy:

```bash
kubectl exec -it kafka-client-test -n kafka -- sh
nslookup kafka-cluster-kafka-bootstrap
```

Output:

```text
Server:         10.96.0.10
Address:        10.96.0.10:53

** server can't find kafka-cluster-kafka-bootstrap.cluster.local: NXDOMAIN

Name:   kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local
Address: 10.96.225.53

** server can't find kafka-cluster-kafka-bootstrap.svc.cluster.local: NXDOMAIN
```

### 5.1. Dòng `Server`

```text
Server:  10.96.0.10
Address: 10.96.0.10:53
```

Pod đang gửi DNS query tới CoreDNS Service của Kubernetes ở port UDP/TCP 53.

### 5.2. Dòng `NXDOMAIN` đầu tiên

```text
server can't find kafka-cluster-kafka-bootstrap.cluster.local: NXDOMAIN
```

BusyBox `nslookup` đang thử một candidate dựa trên search path. Tên:

```text
kafka-cluster-kafka-bootstrap.cluster.local
```

không phải DNS name đúng của Service này, vì thiếu `.kafka.svc`. CoreDNS trả `NXDOMAIN`, nghĩa là candidate đó không tồn tại.

Đây không có nghĩa short name cuối cùng thất bại.

### 5.3. Dòng thành công

```text
Name:    kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local
Address: 10.96.225.53
```

Đây là phần quan trọng nhất. Short name đã được mở rộng thành full Service DNS và resolve đúng ClusterIP `10.96.225.53`.

Vì vậy kết luận DNS là:

```text
DNS resolution: SUCCESS
```

### 5.4. Các `NXDOMAIN` tiếp theo

BusyBox có thể tiếp tục in lỗi cho các candidate khác trong search list, ví dụ:

```text
kafka-cluster-kafka-bootstrap.svc.cluster.local
```

Candidate này cũng thiếu namespace `kafka`, nên không tồn tại. Việc BusyBox in các dòng `NXDOMAIN` xen giữa output thành công là behavior của tiện ích resolver, không phải bằng chứng Service hỏng.

Để kiểm tra rõ hơn, dùng full DNS name:

```bash
kubectl exec kafka-client-test -n kafka -- \
  nslookup kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local
```

Output mong đợi sẽ tập trung vào:

```text
Name:    kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local
Address: 10.96.225.53
```

Hoặc dùng `getent hosts` nếu image có lệnh này:

```bash
kubectl exec kafka-client-test -n kafka -- \
  getent hosts kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local
```

## 6. Giải thích output `nc`

Lệnh:

```bash
nc -zv kafka-cluster-kafka-bootstrap 9092
```

Output:

```text
kafka-cluster-kafka-bootstrap (10.96.225.53:9092) open
```

Đọc từng phần:

- `kafka-cluster-kafka-bootstrap`: tên Service đã resolve.
- `10.96.225.53`: ClusterIP của Service.
- `9092`: Kafka client port.
- `open`: TCP connection tới Service thành công.

Kết luận:

```text
DNS resolution       = thành công
TCP connection 9092  = thành công
```

Nhưng `nc` không nói rằng Kafka protocol đã hoạt động hoàn chỉnh. `nc` chỉ mở socket rồi đóng; nó không gửi Kafka metadata request.

## 7. Ba lớp kiểm tra connectivity

### Lớp 1: DNS

```bash
kubectl exec kafka-client-test -n kafka -- \
  nslookup kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local
```

Nếu thất bại: kiểm tra CoreDNS, namespace, Service name và Pod DNS config.

### Lớp 2: TCP

```bash
kubectl exec kafka-client-test -n kafka -- \
  nc -zv kafka-cluster-kafka-bootstrap 9092
```

Nếu DNS thành công nhưng TCP thất bại: kiểm tra Service port, endpoint, NetworkPolicy, readiness và Kafka listener.

### Lớp 3: Kafka protocol

TCP open chưa đủ. Cần chạy Kafka client thực sự:

```bash
kubectl exec -it kafka-client-test -n kafka -- sh
```

Image `busybox` hiện tại chỉ phù hợp kiểm tra DNS/TCP; nó thường không có Kafka CLI. Dùng một Kafka Pod có binary `/opt/kafka/bin` hoặc tạo client image có Kafka tools:

```bash
kubectl exec -it <kafka-pod-name> -n kafka -- \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-cluster-kafka-bootstrap:9092 \
  --list
```

Nếu command nhận metadata và trả về topic, Kafka protocol connectivity đã hoạt động.

## 7.1. Thực hành với Kafka client Pod độc lập

`busybox` chỉ có các công cụ network như `nslookup` và `nc`. Để kiểm tra Kafka protocol, dùng manifest:

```text
clients/kafka-cli-test.yaml
```

Pod này dùng cùng Kafka image/version với cluster hiện tại:

```text
quay.io/strimzi/kafka:1.2.0-kafka-4.2.0
```

Image có Kafka CLI trong `/opt/kafka/bin`. Pod chỉ chạy `sleep` để giữ container sống; nó không phải broker và không tạo Kafka node mới.

### Tạo client Pod

```bash
kubectl apply -f clients/kafka-cli-test.yaml
kubectl get pod kafka-cli-test -n kafka -w
```

Chờ đến khi thấy:

```text
READY   STATUS
1/1     Running
```

Nếu Pod ở `ImagePullBackOff`, kiểm tra image registry và Events:

```bash
kubectl describe pod kafka-cli-test -n kafka
kubectl get events -n kafka --sort-by=.lastTimestamp
```

### Kiểm tra Kafka CLI trong client Pod

```bash
kubectl exec kafka-cli-test -n kafka -- \
  ls /opt/kafka/bin/kafka-*-*.sh
```

Hoặc kiểm tra một command cụ thể:

```bash
kubectl exec kafka-cli-test -n kafka -- \
  /opt/kafka/bin/kafka-topics.sh --help
```

### Kiểm tra DNS và TCP từ đúng client Pod

```bash
kubectl exec kafka-cli-test -n kafka -- \
  getent hosts kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local

kubectl exec kafka-cli-test -n kafka -- \
  nc -zv kafka-cluster-kafka-bootstrap 9092
```

Nếu image không có `getent` hoặc `nc`, bỏ qua hai lệnh này và chạy Kafka CLI ở bước tiếp theo. Mục tiêu chính của Pod này là kiểm tra Kafka protocol.

### Kiểm tra Kafka protocol bằng `kafka-topics.sh`

```bash
kubectl exec kafka-cli-test -n kafka -- \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-cluster-kafka-bootstrap:9092 \
  --list
```

Nếu output có `orders`, client đã:

```text
resolve được Service
        |
        v
kết nối được TCP 9092
        |
        v
nhận và xử lý được Kafka metadata
```

Describe topic:

```bash
kubectl exec kafka-cli-test -n kafka -- \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-cluster-kafka-bootstrap:9092 \
  --describe --topic orders
```

Output cần đọc các cột `Partition`, `Leader`, `Replicas` và `Isr`.

### Produce message từ client Pod độc lập

Mở producer interactive:

```bash
kubectl exec -it kafka-cli-test -n kafka -- \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-cluster-kafka-bootstrap:9092 \
  --topic orders
```

Nhập từng dòng, mỗi dòng là một record:

```text
client-pod-order-001
client-pod-order-002
client-pod-order-003
```

Dừng producer bằng `Ctrl+C`. Producer này chạy trong `kafka-cli-test`, nhưng record được ghi vào Kafka broker thông qua bootstrap Service.

### Consume message từ client Pod độc lập

Ở terminal khác, chạy consumer group mới:

```bash
kubectl exec -it kafka-cli-test -n kafka -- \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-cluster-kafka-bootstrap:9092 \
  --topic orders \
  --group cli-test-group \
  --from-beginning
```

Bạn có thể thấy các record đã có từ trước và các record mới. Vì `cli-test-group` là group mới, nó chưa có committed offset trước đó.

Kiểm tra offset của group:

```bash
kubectl exec kafka-cli-test -n kafka -- \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-cluster-kafka-bootstrap:9092 \
  --describe --group cli-test-group
```

### Vì sao test này đáng tin cậy hơn `nc`?

Hai lệnh kiểm tra các lớp khác nhau:

```text
busybox + nslookup -> DNS
busybox + nc       -> TCP socket
kafka-cli-test     -> Kafka metadata, produce, consume, offset
```

Nếu `nc` thành công nhưng `kafka-topics.sh` lỗi, Service/network cơ bản vẫn có thể đúng; lỗi cần tìm ở Kafka listener, advertised broker address, authentication hoặc Kafka protocol configuration.

### Dọn dẹp client Pod

Sau khi hoàn thành:

```bash
kubectl delete pod kafka-cli-test -n kafka
```

Lệnh này chỉ xóa client Pod. Nó không xóa Kafka cluster, topic, PVC hoặc record đã ghi vào Kafka.

## 8. Vì sao không dùng `localhost:9092` từ client Pod?

`localhost` luôn trỏ tới network namespace của process hiện tại.

Khi chạy trong Kafka Pod:

```text
localhost:9092 -> Kafka process trong chính Kafka Pod
```

Khi chạy trong `kafka-client-test`:

```text
localhost:9092 -> kafka-client-test Pod
```

Vì `kafka-client-test` không chạy Kafka broker, kết nối tới `localhost:9092` thường bị từ chối.

Từ client Pod phải dùng Service DNS:

```text
kafka-cluster-kafka-bootstrap:9092
```

Trong namespace khác:

```text
kafka-cluster-kafka-bootstrap.kafka:9092
```

## 9. Bootstrap Service và Kafka metadata

Kafka client không chỉ kết nối một lần tới bootstrap Service. Luồng thực tế là:

```text
1. Client kết nối bootstrap Service
2. Client gửi metadata request
3. Kafka trả về broker/partition metadata
4. Client kết nối tới broker leader phù hợp
5. Client produce hoặc consume dữ liệu
```

Vì vậy có thể xảy ra tình huống:

```text
DNS đúng
TCP 9092 open
Kafka client vẫn lỗi
```

Nguyên nhân thường là broker trả về địa chỉ advertised listener mà client không thể resolve hoặc không thể truy cập.

Đây là lý do Kafka networking phức tạp hơn HTTP:

- HTTP client thường gọi một Service và giữ kết nối tới endpoint đó.
- Kafka client bootstrap qua Service nhưng sau đó cần biết địa chỉ từng broker.
- Các broker phải advertise địa chỉ phù hợp với network của client.

Trong lab hiện tại, listener là internal và client Pod nằm trong cluster nên Service DNS nội bộ là đường kết nối đúng.

## 10. Kiểm tra từ namespace khác

Tạo hoặc chạy client trong namespace khác sẽ thay đổi cách resolve short name.

Từ namespace `order-system`, tên sau có thể không resolve đúng:

```text
kafka-cluster-kafka-bootstrap
```

Dùng:

```text
kafka-cluster-kafka-bootstrap.kafka:9092
```

Hoặc:

```text
kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
```

Có thể test bằng Pod tạm:

```bash
kubectl run dns-test \
  --image=busybox:1.36 \
  --restart=Never \
  --rm -it \
  -- sh
```

Sau đó chạy:

```sh
nslookup kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local
nc -zv kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local 9092
```

Lệnh trên chỉ hoạt động nếu namespace hiện tại có quyền và Service tồn tại trong cluster.

## 11. Kiểm tra toàn bộ đường đi

### 11.1. Kiểm tra Kafka Pods

```bash
kubectl get pods -n kafka -o wide
```

Ba Kafka node phải `Ready` trước khi kết luận endpoint khỏe.

### 11.2. Kiểm tra Service

```bash
kubectl get svc kafka-cluster-kafka-bootstrap -n kafka -o wide
kubectl describe svc kafka-cluster-kafka-bootstrap -n kafka
```

### 11.3. Kiểm tra EndpointSlice

```bash
kubectl get endpointslice -n kafka \
  -l kubernetes.io/service-name=kafka-cluster-kafka-bootstrap
```

### 11.4. Kiểm tra DNS

```bash
kubectl exec kafka-client-test -n kafka -- \
  nslookup kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local
```

### 11.5. Kiểm tra TCP

```bash
kubectl exec kafka-client-test -n kafka -- \
  nc -zv kafka-cluster-kafka-bootstrap 9092
```

### 11.6. Kiểm tra Kafka protocol

```bash
kubectl exec -it <kafka-pod-name> -n kafka -- \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-cluster-kafka-bootstrap:9092 \
  --describe --topic orders
```

## 12. Troubleshooting theo triệu chứng

### DNS báo `NXDOMAIN` cho full name

Kiểm tra Service có đúng tên/namespace không:

```bash
kubectl get svc -n kafka
kubectl get svc kafka-cluster-kafka-bootstrap -n kafka
```

Nếu full name đúng mà vẫn lỗi, kiểm tra CoreDNS:

```bash
kubectl get pods -n kube-system -l k8s-app=kube-dns
kubectl logs -n kube-system -l k8s-app=kube-dns --tail=100
```

### Short name có `NXDOMAIN` nhưng full name resolve được

Kiểm tra `/etc/resolv.conf`, search domains và thử full DNS name. Với output hiện tại, đây là behavior bình thường của BusyBox resolver; dòng thành công là full name có ClusterIP `10.96.225.53`.

### DNS resolve được nhưng `nc` báo refused

Kiểm tra:

```bash
kubectl get svc kafka-cluster-kafka-bootstrap -n kafka
kubectl get endpointslice -n kafka \
  -l kubernetes.io/service-name=kafka-cluster-kafka-bootstrap
kubectl get pods -n kafka -o wide
```

Service có thể không có endpoint ready hoặc listener chưa bind port `9092`.

### `nc` timeout

Kiểm tra NetworkPolicy và route:

```bash
kubectl get networkpolicy -A
kubectl describe pod kafka-client-test -n kafka
```

Nếu dùng cluster nhiều node, kiểm tra network plugin và policy giữa namespace/Pod.

### TCP open nhưng Kafka CLI lỗi metadata

Kiểm tra Kafka broker advertised addresses. Trước tiên kiểm tra listener/Service:

```bash
kubectl get svc -n kafka
kubectl get kafka kafka-cluster -n kafka -o yaml
kubectl describe kafka kafka-cluster -n kafka
```

Client phải resolve và truy cập được cả địa chỉ broker mà metadata response trả về, không chỉ bootstrap Service.

### Client ngoài Kubernetes không kết nối được

`ClusterIP` chỉ dùng trong cluster. Cần thiết kế external listener phù hợp với Minikube, NodePort, LoadBalancer hoặc route khác. Không dùng địa chỉ Pod IP và không coi `kubectl port-forward` là external Kafka networking hoàn chỉnh.

## 13. Bài thực hành có giải thích output

### Bước 1: Xác nhận Service

```bash
kubectl describe svc kafka-cluster-kafka-bootstrap -n kafka
```

Bạn cần tìm:

```text
Type:       ClusterIP
IP:         10.96.225.53
tcp-clients 9092/TCP
Endpoints:  3 broker Pod IPs on 9092
```

Kết luận: Service nội bộ có backend broker.

### Bước 2: Xác nhận client Pod

```bash
kubectl get pod kafka-client-test -n kafka -o wide
```

Kết luận cần có:

```text
READY 1/1
STATUS Running
```

### Bước 3: Xác nhận DNS bằng full name

```bash
kubectl exec kafka-client-test -n kafka -- \
  nslookup kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local
```

Kết luận: full name trả về ClusterIP `10.96.225.53`.

### Bước 4: Xác nhận TCP

```bash
kubectl exec kafka-client-test -n kafka -- \
  nc -zv kafka-cluster-kafka-bootstrap 9092
```

Kết luận: socket TCP tới port client mở.

### Bước 5: Xác nhận Kafka protocol

```bash
kubectl exec -it <kafka-pod-name> -n kafka -- \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-cluster-kafka-bootstrap:9092 \
  --list
```

Kết luận: Kafka client có thể bootstrap và nhận metadata. Nếu thấy `orders`, toàn bộ path client nội bộ đã hoạt động ở mức cơ bản.

## 14. Kết luận từ output hiện tại

Dựa trên output bạn cung cấp:

```text
Service ClusterIP: 10.96.225.53
Service port:      9092
Broker endpoints: 10.244.0.83/.84/.85
DNS full name:     resolved to 10.96.225.53
nc:                open
```

Có thể kết luận:

1. Bootstrap Service tồn tại trong namespace `kafka`.
2. Service selector đã tìm thấy ba Kafka broker Pod.
3. DNS từ `kafka-client-test` hoạt động.
4. TCP từ client Pod tới Kafka Service port `9092` hoạt động.
5. Các dòng `NXDOMAIN` của BusyBox là các candidate search-domain không tồn tại, không phủ định dòng resolve thành công.
6. Chưa thể chỉ bằng `nc` kết luận Kafka protocol và advertised broker addresses hoàn toàn đúng; cần chạy Kafka CLI hoặc client library.

## 15. Checklist hoàn thành

- [ ] Hiểu bootstrap Service là entry point cho Kafka client.
- [ ] Hiểu `ClusterIP` chỉ dùng trong Kubernetes cluster.
- [ ] Đọc được selector và endpoints của Service.
- [ ] Phân biệt port replication `9091` với client port `9092`.
- [ ] Hiểu short DNS name và full DNS name.
- [ ] Hiểu vì sao BusyBox có thể in `NXDOMAIN` xen giữa output thành công.
- [ ] DNS full name resolve về ClusterIP `10.96.225.53`.
- [ ] TCP check tới `9092` báo `open`.
- [ ] Không dùng `localhost:9092` từ Pod client khác.
- [ ] Biết TCP open chưa đảm bảo Kafka metadata connectivity.
- [ ] Đã kiểm tra Kafka protocol bằng Kafka CLI.
- [ ] Hiểu internal listener khác external listener.

## 16. Kết nối sang Phase 7

Phase này dùng `kafka-client-test` để kiểm tra DNS và TCP. Phase tiếp theo có thể tạo producer/consumer application thật trong Pod riêng:

```text
Producer application Pod
        |
        v
kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
        |
        v
Kafka broker nodes
```

Khi đó cần cấu hình `bootstrap.servers`, consumer group, retry, timeout, serialization và xử lý advertised listeners trong application thay vì chỉ dùng các command kiểm tra mạng.