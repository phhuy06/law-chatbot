import { useState, useEffect, useRef } from "react";
import ChatMessage from "./components/ChatMessage";
import ChatInput from "./components/ChatInput";
import { ChatMessage as ChatMessageType, ChatResponse } from "./types";
import "./App.css";

// Mock response cho tuan 1 (chua co backend)
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
      // Goi mock API (tuan sau se thay bang API call that)
      const response = await mockResponse(question);

      // Them message assistant vao list
      const assistantMessage: ChatMessageType = {
        id: (Date.now() + 1).toString(),
        role: "assistant",
        content: response.answer,
        sources: response.sources,
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, assistantMessage]);
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
    } finally {
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
