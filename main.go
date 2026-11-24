package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type ChatPacket struct {
	Username string `json:"username"`
	Text     string `json:"text"`
	Time     string `json:"time"`
}

type ChatRoom struct {
	mu    sync.Mutex
	peers map[*websocket.Conn]string // соединение -> имя пользователя
}

var (
	room     = &ChatRoom{peers: make(map[*websocket.Conn]string)}
	wsConfig = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
)

func nowStr() string { return time.Now().Format("15:04:05") }

// Рассылка JSON-пакета всем участникам
func (cr *ChatRoom) Broadcast(pkt ChatPacket) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	for conn := range cr.peers {
		if err := conn.WriteJSON(pkt); err != nil {
			conn.Close()
			delete(cr.peers, conn)
		}
	}
}

// Отправка приватного сообщения (/msg Кому Текст)
func (cr *ChatRoom) SendPrivate(senderConn *websocket.Conn, sender, target, body string) {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	pkt := ChatPacket{
		Username: fmt.Sprintf("🔒 [ЛС: %s → %s]", sender, target),
		Text:     body,
		Time:     nowStr(),
	}

	found := false
	for conn, name := range cr.peers {
		if strings.EqualFold(name, target) || conn == senderConn {
			_ = conn.WriteJSON(pkt)
			if strings.EqualFold(name, target) {
				found = true
			}
		}
	}

	if !found {
		_ = senderConn.WriteJSON(ChatPacket{
			Username: "Система",
			Text:     fmt.Sprintf("Пользователь '%s' не найден", target),
			Time:     nowStr(),
		})
	}
}

func wsEndpoint(w http.ResponseWriter, r *http.Request) {
	conn, err := wsConfig.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Ошибка Upgrade:", err)
		return
	}
	defer conn.Close()

	// Получаем имя из URL (?user=Иван) или генерируем случайное
	user := strings.TrimSpace(r.URL.Query().Get("user"))
	if user == "" {
		user = fmt.Sprintf("User%d", rand.Intn(900)+100)
	}

	room.mu.Lock()
	room.peers[conn] = user
	room.mu.Unlock()

	// Системное оповещение о входе
	room.Broadcast(ChatPacket{Username: "Система", Text: user + " присоединился к чату", Time: nowStr()})

	for {
		var incoming ChatPacket
		if err := conn.ReadJSON(&incoming); err != nil {
			break
		}

		txt := strings.TrimSpace(incoming.Text)
		if txt == "" {
			continue
		}

		// Проверка на команду приватного сообщения: /msg User123 Привет
		if strings.HasPrefix(txt, "/msg ") {
			parts := strings.SplitN(txt, " ", 3)
			if len(parts) == 3 {
				room.SendPrivate(conn, user, parts[1], parts[2])
			}
			continue
		}

		room.Broadcast(ChatPacket{Username: user, Text: txt, Time: nowStr()})
	}

	// Удаление при отключении и системное уведомление
	room.mu.Lock()
	delete(room.peers, conn)
	room.mu.Unlock()

	room.Broadcast(ChatPacket{Username: "Система", Text: user + " покинул чат", Time: nowStr()})
}

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})
	http.HandleFunc("/ws", wsEndpoint)

	fmt.Println("🚀 WebSocket-чат (pr15) запущен на http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
