export interface Source {
  title: string;       // "Luat Doanh nghiep 2020"
  url: string;         // "https://thuvienphapluat.vn/..."
  doc_number: string;  
}

// 1 message trong khung chat
export interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  content: string;
  sources?: Source[];   
  timestamp: Date;
}

// Request gui len backend
export interface ChatRequest {
  question: string;
}

// Response tu backend
export interface ChatResponse {
  answer: string;
  sources: Source[];
}
