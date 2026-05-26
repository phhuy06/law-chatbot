# Legal Chatbot Frontend

React + TypeScript + Vite frontend cho ChatBot Pháp Luật Việt Nam.

## Cài đặt

```bash
npm install
```

## Cấu hình

Tạo file `.env` từ `.env.example`:

```bash
cp .env.example .env
```

Các biến môi trường:

| Biến | Mặc định | Mô tả |
|------|----------|-------|
| `VITE_API_URL` | `http://localhost:8000` | URL của backend API |
| `VITE_USE_MOCK` | `false` | `true` để dùng mock data, `false` để gọi API thật |

## Chạy Development Server

```bash
npm run dev
```

Mở trình duyệt tại: http://localhost:5173

## Build Production

```bash
npm run build
```

Output sẽ ở thư mục `dist/`.

## Chạy trên Kubernetes

```bash
# Từ thư mục gốc của project
kubectl apply -f k8s/app/frontend.yaml
kubectl port-forward -n law-chatbot svc/frontend 3000:3000
```

Frontend sẽ chạy tại: http://localhost:3000

## Tính năng

-  Giao diện chat responsive (desktop + mobile)
-  Hiển thị nguồn trích dẫn (sources)
-  Auto-scroll khi có tin nhắn mới
-  Loading state khi đang chờ phản hồi
-  Error handling
-  Mock mode để test không cần backend

## Cấu trúc thư mục

```
src/
├── App.tsx              # Component chính
├── App.css              # Styling
├── main.tsx             # Entry point
├── components/
│   ├── ChatMessage.tsx  # Component hiển thị message
│   └── ChatInput.tsx    # Component input + button
└── types/
    └── index.ts         # TypeScript types
```

## API Integration

Backend API endpoint: `POST /api/chat`

Request:
```json
{
  "question": "Câu hỏi của bạn"
}
```

Response:
```json
{
  "answer": "Câu trả lời từ chatbot",
  "sources": [
    {
      "title": "Luật Doanh nghiệp 2020",
      "url": "https://...",
      "doc_number": "59/2020/QH14"
    }
  ]
}
```

## Troubleshooting

### CORS Error
Vite đã cấu hình proxy `/api` -> `http://localhost:8000` trong `vite.config.ts`. Không cần lo về CORS khi chạy dev server.

### Backend không chạy
Set `VITE_USE_MOCK=true` trong `.env` để dùng mock data và test frontend độc lập.

### Port 5173 đã được sử dụng
Vite sẽ tự động chọn port khác (5174, 5175, ...). Kiểm tra terminal để biết port đang dùng.
