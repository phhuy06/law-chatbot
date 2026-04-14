import { ChatMessage as ChatMessageType } from "../types";

interface ChatMessageProps {
  message: ChatMessageType;
}

export default function ChatMessage({ message }: ChatMessageProps) {
  const isUser = message.role === "user";

  return (
    <div className={`message-wrapper ${isUser ? "user" : "assistant"}`}>
      <div className={`message ${isUser ? "user-message" : "assistant-message"}`}>
        {message.isLoading ? (
          <div className="loading-dots">
            <span></span>
            <span></span>
            <span></span>
          </div>
        ) : (
          <div className="message-content">{message.content}</div>
        )}
        
        {message.sources && message.sources.length > 0 && (
          <div className="sources">
            <div className="sources-title">Nguồn trích dẫn:</div>
            <ul className="sources-list">
              {message.sources.map((source, index) => (
                <li key={index}>
                  <a href={source.url} target="_blank" rel="noopener noreferrer">
                    {source.title}
                  </a>
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </div>
  );
}
