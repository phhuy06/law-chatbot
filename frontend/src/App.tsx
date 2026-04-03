import { useState, useEffect, useRef } from "react";
import ChatMessage from "./components/ChatMessage";
import ChatInput from "./components/ChatInput";
import { ChatMessage as ChatMessageType, ChatResponse } from "./types";
import "./App.css";

// API Configuration
const API_BASE = import.meta.env.VITE_API_URL || "http://localhost:8000";
const USE_MOCK = import.meta.env.VITE_USE_MOCK === "true"; // Set to "true" to use mock

// Real API call
const sendQuestion = async (question: string): Promise<ChatResponse> => {
  const res = await fetch(`${API_BASE}/api/chat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ question }),
  });

  if (!res.ok) {
    const error = await res.json().catch(() => ({ detail: "Unknown error" }));
    throw new Error(error.detail || `API error: ${res.status}`);
  }

  return res.json();
};

// Mock response (for testing without backend)
const mockResponse = async (question: string): Promise<ChatResponse> => {
  // Gia lap delay 1 giay
  await new Promise((r) => setTimeout(r, 1000));
  return {
    answer: `Đây là câu trả lời mẫu cho: "${question}". Theo quy định pháp luật Việt Nam, doanh nghiệp cần tuân thủ các quy định về đăng ký kinh doanh, thuế, và các nghĩa vụ pháp lý khác.`,
    sources: [
      {
        title: "Luật Doanh nghiệp 2020",
        url: "https://thuvienphapluat.vn/van-ban/Doanh-nghiep/Luat-Doanh-nghiep-2020-59-2020-QH14-427301.aspx",
        doc_number: "59/2020/QH14",
      },
      {
        title: "Bộ luật Dân sự 2015",
        url: "https://thuvienphapluat.vn/van-ban/Quyen-dan-su/Bo-luat-Dan-su-2015-91-2015-QH13-296543.aspx",
        doc_number: "91/2015/QH13",
      },
    ],
  };
};

export default function App() {
  const [messages, setMessages] = useState<ChatMessageType[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  // Auto-scroll xuong cuoi khi co message moi
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const handleSend = async (question: string) => {
    // Them message user vao list
    const userMessage: ChatMessageType = {
      id: Date.now().toString(),
      role: "user",
      content: question,
      timestamp: new Date(),
    };
    setMessages((prev) => [...prev, userMessage]);
    setIsLoading(true);

    try {
      // Goi API (real hoac mock tuy theo config)
      const response = USE_MOCK 
        ? await mockResponse(question)
        : await sendQuestion(question);

      // Tao message assistant rong
      const assistantMessageId = (Date.now() + 1).toString();
      const assistantMessage: ChatMessageType = {
        id: assistantMessageId,
        role: "assistant",
        content: "",
        sources: response.sources,
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, assistantMessage]);

      const fullText = response.answer;
      let currentIndex = 0;

      const typingInterval = setInterval(() => {
        if (currentIndex < fullText.length) {
          currentIndex++;
          setMessages((prev) =>
            prev.map((msg) =>
              msg.id === assistantMessageId
                ? { ...msg, content: fullText.slice(0, currentIndex) }
                : msg
            )
          );
        } else {
          clearInterval(typingInterval);
          setIsLoading(false);
        }
      }, 20); // 20ms cho moi chu (co the dieu chinh)
    } catch (error) {
      console.error("Error:", error);
      // Hien thi loi neu co
      const errorMessage: ChatMessageType = {
        id: (Date.now() + 1).toString(),
        role: "assistant",
        content: "Xin lỗi, đã có lỗi xảy ra. Vui lòng thử lại.",
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, errorMessage]);
      setIsLoading(false);
    }
  };

  return (
    <div className="app">
      <header className="app-header">
        <h1>ChatBot Pháp Luật Việt Nam</h1>
      </header>

      <div className="messages-container">
        {messages.length === 0 && (
          <div className="welcome-message">
            <h2>Chào mừng bạn đến với ChatBot Pháp Luật!</h2>
            <p>Hãy đặt câu hỏi về pháp luật Việt Nam để bắt đầu.</p>
          </div>
        )}
        {messages.map((message) => (
          <ChatMessage key={message.id} message={message} />
        ))}
        <div ref={messagesEndRef} />
      </div>

      <footer className="app-footer">
        <ChatInput onSend={handleSend} disabled={isLoading} />
      </footer>
    </div>
  );
}
