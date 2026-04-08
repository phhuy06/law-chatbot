import { useState, useEffect, useRef } from "react";
import ChatMessage from "./components/ChatMessage";
import ChatInput from "./components/ChatInput";
import { ChatMessage as ChatMessageType, ChatResponse } from "./types";
import "./App.css";

// API Configuration
const API_BASE = import.meta.env.VITE_API_URL || "";

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

    // Show loading dots while waiting for API
    const loadingId = (Date.now() + 1).toString();
    const loadingMessage: ChatMessageType = {
      id: loadingId,
      role: "assistant",
      content: "...",
      isLoading: true,
      timestamp: new Date(),
    };
    setMessages((prev) => [...prev, loadingMessage]);

    try {
      const response = await sendQuestion(question);

      // Replace loading message with real response
      const assistantMessageId = loadingId;
      setMessages((prev) =>
        prev.map((msg) =>
          msg.id === loadingId
            ? { ...msg, content: "", sources: response.sources, isLoading: false }
            : msg
        )
      );

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
      setMessages((prev) =>
        prev.map((msg) =>
          msg.id === loadingId
            ? { ...msg, content: "Xin lỗi, đã có lỗi xảy ra. Vui lòng thử lại.", isLoading: false }
            : msg
        )
      );
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
