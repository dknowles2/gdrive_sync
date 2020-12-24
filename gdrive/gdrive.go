package gdrive

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
)

// TODO(dknowles): Start a web server to do the OAuth exchange redirect.

var tokenFile = flag.String("token_file", "/data/token.json", "Path to the token.json cache")

func New(ctx context.Context, credsFile string) (*drive.Service, error) {
	b, err := ioutil.ReadFile(credsFile)
	if err != nil {
		return nil, fmt.Errorf("unable to read client secret file: %w", err)
	}

	// If modifying these scopes, delete your previously saved token.json.
	config, err := google.ConfigFromJSON(b, drive.DriveScope)
	if err != nil {
		return nil, fmt.Errorf("unable to parse client secret file to config: %w", err)
	}

	// The file token.json stores the user's access and refresh tokens, and is
	// created automatically when the authorization flow completes for the first
	// time.
	token, err := getTokenFromFile()
	if err != nil {
		token, err = getTokenFromWeb(ctx, config)
		if err != nil {
			return nil, err
		}
	}
	client := config.Client(ctx, token)

	srv, err := drive.New(client)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve Drive client: %w", err)
	}

	return srv, nil
}

func getTokenFromFile() (*oauth2.Token, error) {
	f, err := os.Open(*tokenFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}

func getTokenFromWeb(ctx context.Context, config *oauth2.Config) (*oauth2.Token, error) {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the authorization code: \n%v\n", authURL)

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		return nil, fmt.Errorf("unable to read authorization code %w", err)
	}

	token, err := config.Exchange(ctx, authCode)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve token from web %w", err)
	}

	log.Printf("Saving credential file to: %s\n", *tokenFile)
	f, err := os.OpenFile(*tokenFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, fmt.Errorf("unable to cache oauth token: %w", err)
	}
	defer f.Close()
	json.NewEncoder(f).Encode(token)
	return token, nil
}

func GetFolderId(d *drive.Service, n string) (string, error) {
	q := fmt.Sprintf("name=\"%s\" and mimeType=\"application/vnd.google-apps.folder\"", n)
	r, err := d.Files.List().Q(q).Do()
	if err != nil {
		return "", fmt.Errorf("unable to retrieve Drive folder: %w", err)
	}
	for _, f := range r.Files {
		if f.Name == n {
			return f.Id, nil
		}
	}
	return "", fmt.Errorf("unable to find folder: %s", n)
}
