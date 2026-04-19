import { useState, useRef, KeyboardEvent } from "react";

interface ChatInputProps {
  onSend: (question: string) => void;
  disabled: boolean;
}

export default function ChatInput({ onSend, disabled }: ChatInputProps) {
  const [input, setInput] = useState("");
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const handleSend = () => {
    if (input.trim() && !disabled) {
      onSend(input.trim());
      setInput("");
      setTimeout(() => textareaRef.current?.focus(), 0);
    }
  };

  const handleKeyPress = (e: KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <div className="chat-input-container">
      <textarea
        ref={textareaRef}
        className="chat-input"
        value={input}
        onChange={(e) => setInput(e.target.value)}
        onKeyPress={handleKeyPress}
        placeholder={disabled ? "Đang trả lời..." : "Nhập câu hỏi của bạn..."}
        disabled={disabled}
        rows={2}
      />
      <button
        className="send-button"
        onClick={handleSend}
        disabled={disabled || !input.trim()}
      >
        Gửi
      </button>
    </div>
  );
}
