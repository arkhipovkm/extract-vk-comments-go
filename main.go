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
	Error_code int
	Error_msg  string
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
	ID         int
	First_name string
	Last_name  string
	Sex        int
	Bdate      string
	City       VkCity
	Country    VkCountry
}

type VkCommentItem struct {
	ID               int
	From_id          int
	Date             int
	Text             string
	Likes            VkLikes
	Reply_to_user    int
	Reply_to_comment int
	Profile          *VkProfile
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
	ID        int
	From_id   int
	Owner_id  int
	Date      int
	Post_type string
	Text      string
	Likes     VkLikes
	Comments  VkComments
	Reposts   VkReposts
	Views     VkViews
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

	var filename string
	var err error
	var body []byte
	var previousValue int

	filename = filepath.Join(".data", "PostsCounter.txt")
	body, err = ioutil.ReadFile(filename)
	if err != nil {
		previousValue = 0
		err = nil
	} else {
		previousValue, err = strconv.Atoi(string(body))
		if err != nil {
			return err
		}
	}
	PostsCounter += uint64(previousValue)

	filename = filepath.Join(".data", "ProfilesCounter.txt")
	body, err = ioutil.ReadFile(filename)
	if err != nil {
		previousValue = 0
		err = nil
	} else {
		previousValue, err = strconv.Atoi(string(body))
		if err != nil {
			return err
		}
	}
	ProfilesCounter += uint64(previousValue)

	filename = filepath.Join(".data", "CommentsCounter.txt")
	body, err = ioutil.ReadFile(filename)
	if err != nil {
		previousValue = 0
		err = nil
	} else {
		previousValue, err = strconv.Atoi(string(body))
		if err != nil {
			return err
		}
	}
	CommentsCounter += uint64(previousValue)

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
			log.Fatalln(vkResponse.Error.Error_msg)
		}
		if err != nil {
			panic(err)
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
				profilesMap[profile.ID] = &profile
			}
		}

		for _, item := range vkResponse.Response.Items {
			for _, comment := range item.Comments.Items {
				if comment.Text != "" {
					comment.Profile = profilesMap[comment.From_id]
					filename := filepath.Join(".data", "comments", item.GroupID, item.PostID, strconv.Itoa(comment.ID)+".json")
					err = dump(filename, comment)
					if err != nil {
						panic(err)
					}
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
	}
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

	body, err := ioutil.ReadFile("groups.txt")
	if err != nil {
		panic(err)
	}

	logStats()
	go handleInterrupt()

	wg := &sync.WaitGroup{}
	for _, groupID := range strings.Split(string(body), "\n") {
		wg.Add(1)
		go parseGroup(groupID)
	}
	wg.Wait()
}
