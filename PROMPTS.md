# Prompts (same as in tests/prompt_cases.json)

Copy/paste into the web page at `http://127.0.0.1:8000`.

---

1. **sr_quota_between_bg_ro**
   ```
   4 dogadjaja sa kvotom izmedju 1.3 i 1.8 ,2 iz bugarske i dva iz rumunije
   ```

2. **en_total_product_between_5_9**
   ```
   give me 3 matches with total value between 5 and 9
   ```

3. **en_quota_bg_ro_won_over_1_5**
   ```
   give me 3 matchs from Bulgaria and 3 from romania where win is over 1.5
   ```

4. **en_timewindow_totalodd_bg_2025_02_25**
   ```
   I need 2 matches that starts between 12:00 and 17:00 on 25.02.2025 from bulgaria with total odd less then 2
   ```

5. **en_after_time_totalodd_ro_2025_02_28_range_4_6**
   ```
   I need 4 to 6 matches that starts after 12:00 28.02.2025 from romania with total odd less then 2
   ```

6. **sr_total_gt_ro_two_matches**
   ```
   daj mi dvije utakmice iz Rumunije sa ukupnom vrijednoscu vecom od 3
   ```

7. **en_value_between_and_include_match**
   ```
   give me 4 matches with value between 3 and 5 and one more match Anand vs Radjabov won
   ```

8. **nonsense_returns_friendly_error**
   ```
   blabla bla something wrong
   ```

9. **bg_two_matches_total_lt_2**
   ```
   Трябват ми 2 мача от България с общ коефициент по-малък от 2
   ```

10. **sr_19_not_started_after_submit_total_lte_1200**
    ```
    Daj mi 19 utakmica sa ukupnom kovtom ne vecom od 1200 i da utakmice nisu startovale nakon submitovanja ovog teksta
    2026-02-20 11:55:00
    ```

11. **en_england_3_matches_total_odd_3_6**
    ```
    I need 3 matches from Engleand 1 league with total odd betwen 3-6
    ```

12. **sr_tiket_10_parova_3_dana_kvote_1_5_1_8**
    ```
    Sastavi mi tiket od 10 parova za naredna tri dana, a da svaki par pocinje u minimalnom razmaku od dva sata, sa kvotama između 1.5-1.8
    ```

13. **sr_tiket_10_parova_kvote_1_3_1_5**
    ```
    Sastavi mi tiket od 10 parova sa kvotama od 1.3-1.5
    ```

14. **sr_kvotu_8_parova_danas_max_5**
    ```
    Sastavi mi kvotu 8 parovi za danas Max 5 parova
    ```

15. **sr_kvotu_8_parova_danas_max_5_dup** (same text as 14)
    ```
    Sastavi mi kvotu 8 parovi za danas Max 5 parova
    ```

16. **sr_tiket_6_parova_kv_10**
    ```
    Sastavi mi tiket kombinacija golova i konacnog ishoda od 6 parova kv 10
    ```

17. **en_england_league_24_round_0_2_goals**
    ```
    all matches for england league in 24 round to finish 0-2 goals
    ```

18. **sr_3_fudbal_3_kosarka_ukupna_kvota_11_pojedinacna_do_3**
    ```
    3 utakmice iz fudbala i tri iz kosarke sa ukupnom kvotom 11 i pojedinacnom kovtom ne vecom od 3
    ```

19. **sr_manceter_bajern_barselona_dobijaju**
    ```
    Manceter City, Bajern Minken I Barselona dobijaju
    ```
