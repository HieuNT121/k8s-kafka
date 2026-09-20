# Phase 2: Cài đặt và tìm hiểu Strimzi Kafka Operator

Phase này cài Strimzi Cluster Operator vào namespace `kafka` và tìm hiểu cách Operator mở rộng Kubernetes để quản lý Apache Kafka. Chúng ta chưa tạo Kafka cluster, chưa tạo broker, topic hoặc user. Kết quả cuối phase là Kubernetes nhận biết các resource Kafka và Strimzi Operator đang chạy ổn định.

## 1. Mục tiêu học tập

Sau Phase 2, bạn có thể:

- Giải thích Strimzi Operator khác Kafka broker như thế nào.
- Phân biệt CRD và Custom Resource.
- Mô tả reconciliation loop giữa desired state và actual state.
- Cài Cluster Operator vào namespace `kafka`.
- Kiểm tra Deployment, Pod, CRD và API resource do Strimzi đăng ký.
- Đọc log và mô tả Deployment của Operator.
- Nhận biết ServiceAccount, RBAC, Role, ClusterRole và binding của Operator.
- Xác nhận cluster chưa có Kafka broker sau khi cài Operator.

## 2. Phạm vi của phase

### Đã thực hiện

```text
Kubernetes cluster
        |
        +-- namespace kafka
                |
                +-- Strimzi Cluster Operator
                +-- Strimzi CRDs
```

### Chưa thực hiện

```text
Kafka Cluster
Kafka Broker
KafkaNodePool
KafkaTopic
KafkaUser
Producer / Consumer
```

Operator là control plane logic. Kafka cluster mới là data plane chứa broker, topic, partition, record và offset. Việc Operator đang `Running` không có nghĩa là Kafka đã được triển khai.

## 3. Kiểm tra điều kiện đầu vào

Đảm bảo đang đứng ở thư mục gốc project:

```bash
pwd
```

Kiểm tra Minikube, node và namespace:

```bash
minikube status
kubectl get nodes
kubectl get namespace kafka
```

Kết quả cần thỏa mãn:

- `host`, `kubelet` và `apiserver` của Minikube ở trạng thái `Running`.
- Node `minikube` ở trạng thái `Ready`.
- Namespace `kafka` ở trạng thái `Active`.

Xác nhận `kubectl` đang dùng đúng context:

```bash
kubectl config current-context
kubectl cluster-info
```

Nếu dùng Minikube, context thường là `minikube`. Không tiếp tục apply manifest nếu context đang trỏ đến cluster khác với cluster bạn dự định học tập.

## 4. Strimzi Operator là gì?

Kubernetes mặc định biết cách quản lý các resource như:

```text
Pod, Deployment, StatefulSet, Service, ConfigMap, Secret, PVC
```

Kubernetes không tự có kiến thức về Kafka. Nó không biết một Kafka cluster cần bao nhiêu broker, cách cấu hình listener, cách xử lý rolling update hoặc cách kiểm tra trạng thái Kafka.

Strimzi bổ sung kiến thức đó bằng một Kubernetes Operator. Operator gồm controller code và các resource Kubernetes cần thiết để:

- Theo dõi Kafka custom resources.
- Tạo và cập nhật workload Kafka.
- Quản lý cấu hình, network và storage theo khai báo.
- Báo cáo trạng thái resource.
- Thực hiện reconciliation khi trạng thái thực tế lệch với trạng thái mong muốn.

Mô hình tổng quát:

```text
Kubernetes API Server
          |
          v
Strimzi Cluster Operator
          |
          v
Kafka custom resource
          |
          v
Kafka resources: Pods, Services, Storage, Config
```

## 5. CRD và Custom Resource

### 5.1. CRD là gì?

CRD, viết tắt của Custom Resource Definition, là cách đăng ký một loại resource mới vào Kubernetes API.

Kubernetes built-in resource:

```yaml
apiVersion: apps/v1
kind: Deployment
```

Sau khi cài Strimzi, Kubernetes có thể nhận biết thêm các loại resource như:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
```

CRD mô tả schema, API group, version và phạm vi namespace của Custom Resource. CRD không phải là một Kafka cluster đang chạy; nó chỉ dạy Kubernetes cách nhận biết một loại object mới.

### 5.2. Custom Resource là gì?

Custom Resource là object cụ thể được tạo dựa trên CRD. Ví dụ, một object `kind: Kafka` có thể mô tả cluster tên `my-cluster`.

```text
CRD: định nghĩa loại resource Kafka
CR:  một Kafka object cụ thể
```

Ở Phase 3 chúng ta mới apply Kafka CR. Trong Phase 2, chỉ cài CRD và Operator.

### 5.3. Các CRD thường gặp của Strimzi

| CRD | Mục đích |
| --- | --- |
| `Kafka` | Mô tả Kafka cluster và cấu hình node |
| `KafkaTopic` | Mô tả topic, partition và replication |
| `KafkaUser` | Mô tả user, authentication và authorization |
| `KafkaConnect` | Mô tả Kafka Connect cluster |
| `KafkaConnector` | Mô tả connector chạy trong Kafka Connect |
| `KafkaRebalance` | Mô tả yêu cầu cân bằng lại partition |

Tên resource dạng `kafkas.kafka.strimzi.io` có thể đọc như sau:

```text
kafkas       = plural resource name
kafka        = API group
strimzi.io   = group domain
```

## 6. Reconciliation loop

Kubernetes sử dụng mô hình declarative. Người dùng mô tả desired state; controller quan sát actual state và cố đưa actual state về desired state.

Ví dụ:

```text
Desired state: 3 Kafka nodes
Actual state:  2 Kafka nodes
```

Strimzi Operator sẽ nhận thấy sự khác biệt và thực hiện các thao tác cần thiết để đạt trạng thái mong muốn:

```text
Kafka CR
   |
   v
Strimzi reconciliation
   |
   +-- tạo node còn thiếu
   +-- cập nhật cấu hình
   +-- kiểm tra trạng thái
   v
Actual state tiến gần desired state
```

Điểm quan trọng:

- Operator không chỉ chạy một lần khi bạn apply YAML.
- Operator liên tục watch resource và các thay đổi liên quan.
- Xóa hoặc sửa resource con có thể khiến Operator tạo lại hoặc điều chỉnh resource đó.
- Trạng thái `READY` của custom resource không giống trạng thái `Running` của Pod.

## 7. Kiểm tra CRD trước khi cài

Chạy:

```bash
kubectl get crd | grep -i kafka
```

Nếu chưa cài Strimzi, command có thể không trả về dòng nào. Đây là kết quả bình thường.

Kiểm tra API resource hiện tại:

```bash
kubectl api-resources | grep -i kafka
```

Sau khi cài Operator, hai command trên sẽ hiển thị các resource Kafka do Strimzi đăng ký.

## 8. Cài Strimzi Cluster Operator

### 8.1. Cách cài nhanh cho môi trường học tập

Đảm bảo namespace tồn tại:

```bash
kubectl get namespace kafka
```

Cài manifest release của Strimzi vào namespace `kafka`:

```bash
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
```

Ý nghĩa của lệnh:

```text
Tải installation manifest
        |
        v
Tạo CRD, RBAC, Deployment và resource liên quan
        |
        v
Strimzi Cluster Operator chạy trong namespace kafka
```

Manifest cài đặt có thể bao gồm resource cluster-scoped như CRD, ClusterRole và ClusterRoleBinding. Vì vậy `-n kafka` không biến mọi resource thành namespaced resource; nó chủ yếu xác định namespace cho các resource namespaced.

### 8.2. Pin version trong project thực tế

URL `latest` phù hợp để thử nghiệm nhanh nhưng không phù hợp cho build có tính lặp lại. Version mới có thể thay đổi API, schema hoặc hành vi cài đặt.

Khi project đã chọn version, hãy dùng URL release cụ thể của Strimzi thay vì `latest`, đồng thời ghi version đó vào README hoặc tài liệu môi trường. Trước khi cài, đọc release notes để kiểm tra compatibility giữa Strimzi, Kafka và Kubernetes.

### 8.3. Kiểm tra kết quả apply

Ngay sau khi tạo resource, kiểm tra các resource chính:

```bash
kubectl get deployment -n kafka
kubectl get pods -n kafka
kubectl get crd | grep -i kafka
```

Operator có thể cần một khoảng thời gian để pull image và chuyển sang `Running`. Không đánh giá thành công chỉ dựa trên việc lệnh `kubectl create` không báo lỗi; cần kiểm tra `READY` và `STATUS`.

## 9. Kiểm tra Cluster Operator

### 9.1. Deployment và Pod

```bash
kubectl get deployment -n kafka
kubectl get pods -n kafka -o wide
```

Kết quả mong đợi có Deployment và Pod tương tự:

```text
NAME                       READY   UP-TO-DATE   AVAILABLE
strimzi-cluster-operator   1/1     1            1
```

Tên Pod có suffix ngẫu nhiên nên không nên hard-code tên Pod trong script.

Các trạng thái cần hiểu:

- `Pending`: Pod chưa được schedule hoặc chưa đủ tài nguyên.
- `ContainerCreating`: Kubernetes đang tạo container, mount volume hoặc network.
- `Running` nhưng `READY 0/1`: container chạy nhưng readiness probe chưa thành công.
- `CrashLoopBackOff`: container khởi động rồi bị lỗi lặp lại.
- `ImagePullBackOff`: không pull được image.

### 9.2. Chờ Operator sẵn sàng

Có thể dùng lệnh chờ thay vì kiểm tra thủ công nhiều lần:

```bash
kubectl wait --for=condition=Available \
  deployment/strimzi-cluster-operator \
  -n kafka \
  --timeout=180s
```

Nếu Deployment có tên khác, lấy tên thực tế bằng `kubectl get deployment -n kafka` rồi thay vào command.

### 9.3. Kiểm tra toàn bộ namespace

```bash
kubectl get all -n kafka
```

Ở cuối Phase 2, bạn chủ yếu thấy resource của Operator. Chưa thấy Kafka broker là đúng.

## 10. Kiểm tra CRD và API resources

Liệt kê CRD Kafka:

```bash
kubectl get crd | grep -i kafka
```

Xem chi tiết một CRD:

```bash
kubectl describe crd kafkas.kafka.strimzi.io
```

Liệt kê API resource mà API Server biết:

```bash
kubectl api-resources | grep -i kafka
```

Kiểm tra schema và version của CRD bằng YAML:

```bash
kubectl get crd kafkas.kafka.strimzi.io -o yaml
```

Khi đọc output, chú ý:

- `spec.group`: API group, thường là `kafka.strimzi.io`.
- `spec.names`: tên singular, plural và kind.
- `spec.scope`: `Namespaced` hoặc `Cluster`.
- `spec.versions`: các API version được đăng ký.
- `status.conditions`: trạng thái Established và NamesAccepted.

Một số version Strimzi có thể dùng API version khác với ví dụ tài liệu. Luôn ưu tiên version được CRD hiện tại của cluster đăng ký, không sao chép cứng `v1beta2` nếu release đang dùng version khác.

Kiểm tra riêng các resource namespaced:

```bash
kubectl api-resources --api-group=kafka.strimzi.io
```

## 11. Xác nhận chưa có Kafka cluster

Chạy:

```bash
kubectl get kafka -n kafka
```

Kết quả thường là:

```text
No resources found in kafka namespace.
```

Đây là kết quả đúng. CRD cho phép API Server hiểu `kind: Kafka`, nhưng chưa có Kafka custom resource nào được tạo.

Phân biệt ba trạng thái:

```text
CRD tồn tại       = API Server biết loại Kafka resource
Kafka CR tồn tại  = Có khai báo một Kafka cluster
Broker Pod chạy   = Kafka data plane thực sự đang chạy
```

Phase 2 chỉ hoàn thành hai điều đầu tiên ở mức CRD, chưa có Kafka CR và broker Pod.

## 12. Đọc log Operator

Xem 50 dòng log gần nhất:

```bash
kubectl logs deployment/strimzi-cluster-operator \
  -n kafka \
  --tail=50
```

Theo dõi log trực tiếp:

```bash
kubectl logs -f deployment/strimzi-cluster-operator -n kafka
```

Dừng lệnh follow bằng `Ctrl+C`.

Nếu Deployment có nhiều container, chỉ định container khi cần:

```bash
kubectl logs deployment/strimzi-cluster-operator \
  -n kafka \
  -c strimzi-cluster-operator \
  --tail=100
```

Khi debug, tìm các dấu hiệu:

- lỗi authentication hoặc authorization tới API Server;
- lỗi không tìm thấy CRD;
- lỗi parse configuration hoặc environment variable;
- lỗi không watch được resource;
- lỗi image, DNS hoặc kết nối mạng.

Log không phải bằng chứng duy nhất cho trạng thái hệ thống. Đối chiếu log với `kubectl describe`, Events và trạng thái resource.

## 13. Describe Deployment và Pod

Xem Deployment:

```bash
kubectl describe deployment \
  strimzi-cluster-operator \
  -n kafka
```

Chú ý các phần:

- `Replicas` và `Available`.
- `Pod Template`.
- Container image và arguments.
- Environment variables.
- ServiceAccount.
- Events ở cuối output.

Lấy Pod name rồi describe Pod:

```bash
kubectl get pods -n kafka
kubectl describe pod <operator-pod-name> -n kafka
```

Events thường cho manh mối tốt hơn khi Pod chưa chạy, đặc biệt với lỗi scheduling, image pull, volume hoặc probe.

## 14. ServiceAccount và RBAC

### 14.1. Vì sao Operator cần RBAC?

Operator phải gọi Kubernetes API để watch và thay đổi resource. Các thao tác có thể gồm:

```text
get, list, watch, create, update, patch, delete
```

Nếu thiếu quyền, API Server trả về lỗi `403 Forbidden` và Operator không thể reconcile resource.

Mô hình request:

```text
Strimzi Operator
        |
        | Kubernetes API request
        v
Kubernetes API Server
        |
        | RBAC evaluation
        v
Allowed hoặc Forbidden
```

### 14.2. Xem ServiceAccount

```bash
kubectl get serviceaccount -n kafka
```

Lấy ServiceAccount mà Deployment đang sử dụng:

```bash
kubectl get deployment strimzi-cluster-operator \
  -n kafka \
  -o jsonpath='{.spec.template.spec.serviceAccountName}{"\n"}'
```

Dùng chính xác tên trả về từ command trên cho các bước kiểm tra RBAC.

### 14.3. Xem Role và binding

```bash
kubectl get role -n kafka
kubectl get rolebinding -n kafka
kubectl get clusterrole | grep -i strimzi
kubectl get clusterrolebinding | grep -i strimzi
```

Role thường cấp quyền trong một namespace. ClusterRole có thể cấp quyền ở phạm vi cluster hoặc được bind vào namespace tùy cấu hình. ClusterRoleBinding cấp quyền ở phạm vi cluster.

Tên và số lượng resource RBAC có thể khác theo version hoặc cách cài đặt. Không nên coi một tên resource cụ thể là bất biến; hãy lần theo ServiceAccount, RoleBinding và ClusterRoleBinding đang tồn tại.

### 14.4. Kiểm tra bằng `kubectl auth can-i`

Đầu tiên lấy ServiceAccount:

```bash
OPERATOR_SA=$(kubectl get deployment strimzi-cluster-operator \
  -n kafka \
  -o jsonpath='{.spec.template.spec.serviceAccountName}')
```

Kiểm tra quyền đọc Pod:

```bash
kubectl auth can-i list pods \
  --as="system:serviceaccount:kafka:${OPERATOR_SA}"
```

Kiểm tra quyền đọc Kafka resource:

```bash
kubectl auth can-i get kafka \
  --as="system:serviceaccount:kafka:${OPERATOR_SA}"
```

Kiểm tra một quyền tạo Deployment:

```bash
kubectl auth can-i create deployments \
  --as="system:serviceaccount:kafka:${OPERATOR_SA}"
```

Kết quả `yes` cho biết request được phép; `no` cho biết RBAC hiện tại không cho phép request đó. Đây là kiểm tra quyền mô phỏng, không phải lệnh cấp quyền.

## 15. Luồng cài đặt và kiểm tra tổng hợp

Thực hiện theo thứ tự:

```bash
kubectl get namespace kafka
kubectl get crd | grep -i kafka
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl wait --for=condition=Available \
  deployment/strimzi-cluster-operator \
  -n kafka \
  --timeout=180s
kubectl get deployment -n kafka
kubectl get pods -n kafka
kubectl get crd | grep -i kafka
kubectl api-resources --api-group=kafka.strimzi.io
kubectl get kafka -n kafka
kubectl logs deployment/strimzi-cluster-operator \
  -n kafka \
  --tail=50
```

Nếu đã cài trước đó và muốn chạy lại một cách idempotent, có thể dùng `kubectl apply` với manifest version cụ thể. Không nên trộn `create` và `apply` trên cùng một manifest mà không hiểu trạng thái resource hiện tại; `create` sẽ lỗi nếu resource đã tồn tại.

## 16. Troubleshooting

### 16.1. Operator không xuất hiện

Kiểm tra resource và Events:

```bash
kubectl get all -n kafka
kubectl get events -n kafka --sort-by=.lastTimestamp
```

Nếu lệnh cài đặt báo lỗi, đọc nguyên nhân ngay tại output trước khi chạy lại. Có thể một phần resource đã được tạo thành công.

### 16.2. Pod ở trạng thái `Pending`

```bash
kubectl describe pod <operator-pod-name> -n kafka
```

Tập trung vào phần `Events`. Trên Minikube, nguyên nhân thường gặp là thiếu CPU hoặc memory. Kiểm tra node:

```bash
kubectl describe node minikube
kubectl top node
```

`kubectl top` cần Metrics Server; nếu chưa có thì command có thể không hoạt động.

### 16.3. `ImagePullBackOff`

```bash
kubectl describe pod <operator-pod-name> -n kafka
```

Đọc Events để phân biệt lỗi image name, registry, DNS, proxy hoặc network. Kiểm tra Docker daemon và kết nối mạng:

```bash
docker info
minikube ssh -- 'getent hosts quay.io || getent hosts registry.redhat.io'
```

Không nên sửa image tag tùy ý; image phải khớp với manifest Strimzi và version đã chọn.

### 16.4. `CrashLoopBackOff`

Xem log hiện tại và log của lần khởi động trước:

```bash
kubectl logs <operator-pod-name> -n kafka --tail=100
kubectl logs <operator-pod-name> -n kafka --previous --tail=100
```

Sau đó xem `describe` để kiểm tra probe, environment, permission và Events.

### 16.5. CRD không xuất hiện

```bash
kubectl get crd | grep -i kafka
kubectl get events -A --sort-by=.lastTimestamp
```

CRD là cluster-scoped nên không dùng `-n kafka` khi truy vấn CRD. Kiểm tra API Server có phản hồi và manifest cài đặt có thực sự được tải xuống không.

### 16.6. `kubectl get kafka` báo `the server doesn't have a resource type "kafka"`

Điều đó thường có nghĩa CRD chưa được cài hoặc chưa `Established`:

```bash
kubectl get crd kafkas.kafka.strimzi.io
kubectl describe crd kafkas.kafka.strimzi.io
kubectl api-resources --api-group=kafka.strimzi.io
```

Không tạo Kafka CR trước khi API Server nhận biết CRD.

### 16.7. Lỗi quyền `Forbidden`

Kiểm tra ServiceAccount thực tế của Deployment, sau đó dùng `kubectl auth can-i` với đúng identity. Kiểm tra cả RoleBinding và ClusterRoleBinding:

```bash
kubectl get rolebinding -n kafka -o yaml
kubectl get clusterrolebinding -o yaml | grep -B 5 -A 10 -i strimzi
```

Không cấp quyền `cluster-admin` chỉ để che giấu lỗi RBAC. Hãy xác định permission còn thiếu và dùng quyền tối thiểu theo manifest chính thức.

### 16.8. Namespace không đúng

Kiểm tra Pod, Deployment và ServiceAccount:

```bash
kubectl get deployment -A | grep -i strimzi
kubectl get pods -A | grep -i strimzi
```

Installation manifest có thể tạo resource ở namespace khác nếu tham số namespace hoặc nội dung manifest không đúng. Không giả định Operator luôn nằm trong namespace hiện tại; hãy kiểm tra thực tế.

## 17. Bài tập củng cố

### Bài tập 1: Phân biệt CRD và CR

Viết câu trả lời ngắn cho các câu hỏi:

1. CRD dùng để làm gì?
2. Một `Kafka` custom resource khác CRD `kafkas.kafka.strimzi.io` như thế nào?
3. Vì sao có CRD nhưng chưa có Kafka broker?

### Bài tập 2: Quan sát API Server

Chạy:

```bash
kubectl api-resources --api-group=kafka.strimzi.io
kubectl get crd kafkas.kafka.strimzi.io -o jsonpath='{.spec.scope}{"\n"}'
kubectl get crd kafkas.kafka.strimzi.io -o jsonpath='{.status.conditions[*].type}{"\n"}'
```

Ghi lại API group, scope và các condition của CRD.

### Bài tập 3: Theo dõi reconciliation

Ở Phase 2, chưa có Kafka CR để Operator reconcile. Hãy mô tả bằng sơ đồ luồng sẽ xảy ra ở Phase 3:

```text
Kafka CR -> Operator watch -> reconciliation -> Kubernetes resources
```

Ghi rõ resource nào là desired state và resource nào là actual state.

### Bài tập 4: RBAC

Lấy ServiceAccount của Operator và kiểm tra các quyền sau:

```bash
kubectl auth can-i get pods \
  --as="system:serviceaccount:kafka:${OPERATOR_SA}"
kubectl auth can-i list deployments \
  --as="system:serviceaccount:kafka:${OPERATOR_SA}"
kubectl auth can-i watch kafka \
  --as="system:serviceaccount:kafka:${OPERATOR_SA}"
```

Giải thích vì sao Operator cần watch Kafka resource và cần quyền trên các resource Kubernetes khác.

## 18. Checklist hoàn thành Phase 2

- [ ] Minikube đang chạy và node ở trạng thái `Ready`.
- [ ] Context `kubectl` trỏ đến đúng cluster.
- [ ] Namespace `kafka` tồn tại.
- [ ] Strimzi Cluster Operator đã được cài.
- [ ] Deployment Operator có `AVAILABLE` bằng số replica mong muốn.
- [ ] Operator Pod có `READY` là `1/1` và `STATUS` là `Running`.
- [ ] CRD Kafka xuất hiện trong API Server.
- [ ] `kubectl api-resources --api-group=kafka.strimzi.io` trả về resource Strimzi.
- [ ] Đã kiểm tra log và Events của Operator.
- [ ] Đã xác định ServiceAccount và kiểm tra RBAC cơ bản.
- [ ] `kubectl get kafka -n kafka` chưa có resource Kafka nào.
- [ ] Hiểu rằng Operator không phải Kafka broker.

## 19. Kết nối sang Phase 3

Sau Phase 2, kiến trúc đang ở trạng thái:

```text
Kubernetes
    |
    +-- namespace kafka
            |
            +-- Strimzi Cluster Operator
            +-- Kafka CRDs
            +-- chưa có Kafka cluster
```

Phase 3 sẽ tạo Kafka custom resource và theo dõi toàn bộ quá trình:

```text
Kafka CR
   |
   v
Strimzi reconciliation
   |
   v
KafkaNodePool / Kafka nodes
   |
   v
Pods, Services, Storage
   |
   v
KRaft Kafka cluster
```

Trước khi sang phase tiếp theo, hãy giữ lại output của các lệnh `kubectl get crd`, `kubectl get deployment -n kafka` và `kubectl get pods -n kafka`. Đây sẽ là baseline để so sánh khi Kafka cluster được tạo.