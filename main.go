package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var VK_API_ACCESS_TOKEN_USER string = os.Getenv("VK_API_ACCESS_TOKEN_USER")

type VkError struct {
	ErrorCode int    `json:"error_code"`
	ErrorMsg  string `json:"error_msg"`
}

type VkLikes struct {
	Count int
}

type VkReposts struct {
	Count int
}

type VkViews struct {
	Count int
}

type VkComments struct {
	Count int
}

type VkCity struct {
	ID    int
	Title string
}

type VkCountry struct {
	ID    int
	Title string
}

type VkProfile struct {
	ID        int
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Sex       int
	Bdate     string
	Byear     int
	City      VkCity
	Country   VkCountry
}

type VkCommentItem struct {
	ID             int
	FromID         int `json:"from_id"`
	PostID         string
	GroupID        string
	PostURL        string
	Date           int
	Text           string
	Likes          VkLikes
	ReplyToUser    int `json:"reply_to_user"`
	ReplyToComment int `json:"reply_to_comment"`
	Profile        *VkProfile
}

type VkCommentsResp struct {
	Count    int
	Items    []VkCommentItem
	Profiles []VkProfile
}

type VkItem struct {
	GroupID  string
	PostID   string
	Comments VkCommentsResp
}

type VkPostItem struct {
	ID       int
	FromID   int `json:"from_id"`
	OwnerID  int `json:"owner_id"`
	Date     int
	PostType string `json:"post_type"`
	Text     string
	Likes    VkLikes
	Comments VkComments
	Reposts  VkReposts
	Views    VkViews
}

type VkPostsResp struct {
	Count int
	Items []VkPostItem
}

type VkInnerResponse struct {
	Posts VkPostsResp
	Items []VkItem
}

type VkResponse struct {
	Response *VkInnerResponse
	Error    *VkError
}

var mutex *sync.Mutex = &sync.Mutex{}
var limiter <-chan time.Time = time.Tick(333 * time.Millisecond)
var wg sync.WaitGroup = sync.WaitGroup{}

var PostsCounter uint64
var ProfilesCounter uint64
var CommentsCounter uint64

func doGETRequest(uri string, query url.Values) ([]byte, error) {
	var body []byte
	var err error

	if err != nil {
		return nil, err
	}
	queryString := query.Encode()
	uri += "?" + queryString

	mutex.Lock()
	<-limiter
	mutex.Unlock()
	resp, err := http.Get(uri)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return body, err
}

func doVKAPIRequest(methodName string, query url.Values) (VkResponse, error) {
	var vkResponse VkResponse
	var err error

	query.Add("access_token", VK_API_ACCESS_TOKEN_USER)
	query.Add("v", "5.71")

	body, err := doGETRequest("https://api.vk.com/method/"+methodName, query)
	if err != nil {
		return vkResponse, err
	}
	err = json.Unmarshal(body, &vkResponse)
	if err != nil {
		return vkResponse, err
	}
	return vkResponse, err
}

func dump(filename string, v interface{}) error {
	var err error
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	err = os.MkdirAll(filepath.Dir(filename), os.ModePerm)
	if err != nil {
		if err, ok := err.(*os.PathError); ok {
			log.Println(err.Err.Error())
		} else {
			return err
		}
	}
	err = ioutil.WriteFile(filename, data, os.ModePerm)
	if err != nil {
		return err
	}
	return err
}

func dumpCounters() error {
	var filename string
	var err error

	filename = filepath.Join(".data", "PostsCounter.txt")
	err = ioutil.WriteFile(filename, []byte(strconv.Itoa(int(PostsCounter))), os.ModePerm)
	if err != nil {
		return err
	}
	filename = filepath.Join(".data", "ProfilesCounter.txt")
	err = ioutil.WriteFile(filename, []byte(strconv.Itoa(int(ProfilesCounter))), os.ModePerm)
	if err != nil {
		return err
	}
	filename = filepath.Join(".data", "CommentsCounter.txt")
	err = ioutil.WriteFile(filename, []byte(strconv.Itoa(int(CommentsCounter))), os.ModePerm)
	if err != nil {
		return err
	}
	return err
}

func loadCounters() error {
	var err error

	var groupsCount int
	var postsCount int
	var commentsCount int

	groupsDirList, err := ioutil.ReadDir(filepath.Join(".data", "comments"))
	if err != nil {
		return err
	}
	// for group in groups
	for _, groupFileInfo := range groupsDirList {
		if groupFileInfo.IsDir() {
			groupsCount++
			postsDirList, err := ioutil.ReadDir(filepath.Join(".data", "comments", groupFileInfo.Name()))
			if err != nil {
				return err
			}
			// for post in group.posts
			for _, postFileInfo := range postsDirList {
				if postFileInfo.IsDir() {
					postsCount++
					commentsDirList, err := ioutil.ReadDir(filepath.Join(".data", "comments", groupFileInfo.Name(), postFileInfo.Name()))
					if err != nil {
						return err
					}
					// for comment in post.comments
					commentsCount += len(commentsDirList) - 1
				}
			}
		}
	}

	profilesDirList, err := ioutil.ReadDir(filepath.Join(".data", "profiles"))
	if err != nil {
		return err
	}
	profilesCount := len(profilesDirList)

	PostsCounter += uint64(postsCount)
	CommentsCounter += uint64(commentsCount)
	ProfilesCounter += uint64(profilesCount)

	return err
}

func parseGroup(groupID string) error {
	var err error
	var filename string
	var offset int
	filename = filepath.Join(".data", "comments", groupID, "offset.txt")
	body, err := ioutil.ReadFile(filename)
	if err == nil {
		offset, err = strconv.Atoi(string(body))
		if err != nil {
			panic(err)
		}
	}
	log.Printf("Starting parsing group %s from offset %d\n", groupID, offset)
	for {
		query := url.Values{
			"group":  {groupID},
			"offset": {strconv.Itoa(offset)},
			"req":    {"20"},
		}
		vkResponse, err := doVKAPIRequest("execute.getComments", query)
		if vkResponse.Error != nil {
			log.Println(vkResponse.Error.ErrorMsg)
			log.Println("Sleeping for 60 seconds to recover..")
			time.Sleep(60)
			continue
		}
		if err != nil {
			log.Println(err)
			log.Println("Sleeping for 60 seconds to recover..")
			time.Sleep(60)
			continue
		}
		for _, post := range vkResponse.Response.Posts.Items {
			if post.Comments.Count > 0 {
				filename := filepath.Join(".data", "comments", groupID, strconv.Itoa(post.ID), "post.json")
				err = dump(filename, post)
				if err != nil {
					panic(err)
				}
				atomic.AddUint64(&PostsCounter, 1)
			}
		}

		profilesMap := make(map[int]*VkProfile)
		for _, item := range vkResponse.Response.Items {
			for _, profile := range item.Comments.Profiles {
				bparts := strings.Split(profile.Bdate, ".")
				var byear int
				if len(bparts) > 2 {
					byear, _ = strconv.Atoi(bparts[2])
				}
				profile.Byear = byear
				profilesMap[profile.ID] = &profile
			}
		}

		for _, item := range vkResponse.Response.Items {
			for _, comment := range item.Comments.Items {
				if comment.Text != "" {
					comment.Profile = profilesMap[comment.FromID]
					comment.PostID = item.PostID
					comment.GroupID = item.GroupID
					absGroupID := strings.Split(item.GroupID, "-")
					comment.PostURL = "https://vk.com/public" + absGroupID[1] + "?w=wall" + item.GroupID + "_" + item.PostID
					filename := filepath.Join(".data", "comments", item.GroupID, item.PostID, strconv.Itoa(comment.ID)+".json")
					err = dump(filename, comment)
					if err != nil {
						panic(err)
					}
					// id := comment.GroupID + "_" + comment.PostID + "_" + strconv.Itoa(comment.ID)
					// elastic.Fs2DBQueue <- elastic.Fs2DBArgs{
					// 	ID:       id,
					// 	Filename: filename,
					// 	Index:    "comments-1",
					// }
					atomic.AddUint64(&CommentsCounter, 1)
				}
			}
			for _, profile := range item.Comments.Profiles {
				filename := filepath.Join(".data", "profiles", strconv.Itoa(profile.ID)+".json")
				err = dump(filename, profile)
				if err != nil {
					panic(err)
				}
				atomic.AddUint64(&ProfilesCounter, 1)
			}
		}
		offset += len(vkResponse.Response.Posts.Items)
		filename = filepath.Join(".data", "comments", groupID, "offset.txt")
		err = ioutil.WriteFile(filename, []byte(strconv.Itoa(offset)), os.ModePerm)
		if err != nil {
			panic(err)
		}
		filename = filepath.Join(".data", "comments", groupID, "count.txt")
		err = ioutil.WriteFile(filename, []byte(strconv.Itoa(vkResponse.Response.Posts.Count)), os.ModePerm)
		if err != nil {
			panic(err)
		}
		log.Printf("Group %s. Next offset: %d\n", groupID, offset)
		log.Printf("Total Posts: %d. Total Profiles %d. TotalComments %d", PostsCounter, ProfilesCounter, CommentsCounter)
		if offset >= vkResponse.Response.Posts.Count {
			log.Printf("Finish. Parsed %d/%d posts", offset, vkResponse.Response.Posts.Count)
			break
		}
	}
	wg.Done()
	return err
}

func handleInterrupt() {
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sigCh:
		dumpCounters()
		log.Println("Exiting gracefully..")
		os.Exit(0)
	}
}

func logStats() error {
	var err error
	dirList, err := ioutil.ReadDir(filepath.Join(".data", "comments"))
	if err != nil {
		return err
	}
	for _, fileInfo := range dirList {
		if fileInfo.IsDir() {
			body, err := ioutil.ReadFile(filepath.Join(".data", "comments", fileInfo.Name(), "offset.txt"))
			if err != nil {
				log.Printf("No offset data for group %s\n", fileInfo.Name())
				continue
			}
			offset, _ := strconv.Atoi(string(body))

			body, err = ioutil.ReadFile(filepath.Join(".data", "comments", fileInfo.Name(), "count.txt"))
			if err != nil {
				log.Printf("No count data for group %s\n", fileInfo.Name())
				continue
			}
			count, _ := strconv.Atoi(string(body))
			log.Printf("Group %s : %d/%d : %.1f %%\n", fileInfo.Name(), offset, count, 100*float64(offset)/float64(count))
		}
	}
	loadCounters()
	log.Printf("Total Posts: %d\n", PostsCounter)
	log.Printf("Total Profiles: %d\n", ProfilesCounter)
	log.Printf("Total Comments: %d\n", CommentsCounter)
	return err
}

func main() {

	if VK_API_ACCESS_TOKEN_USER == "" {
		body, err := ioutil.ReadFile("access_token.txt")
		if err != nil {
			log.Fatalln("No AccessToken found in the environment.\nVisit https://oauth.vk.com/authorize?client_id=6359340&redirect_uri=https://oauth.vk.com/blank.html&response_type=token to get a token and put it into a file 'access_token.txt' or into a VK_API_ACCESS_TOKEN_USER environmental variable.\nExiting..")
		}
		VK_API_ACCESS_TOKEN_USER = string(body)
	}

	body, err := ioutil.ReadFile("groups.txt")
	if err != nil {
		log.Fatalln("Groups.txt file not found. Exiting..")
	}

	logStats()
	go handleInterrupt()

	for _, groupID := range strings.Split(string(body), "\n") {
		wg.Add(1)
		go parseGroup(groupID)
	}
	// go elastic.ElasticLoop()
	wg.Wait()
}
